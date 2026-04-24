import logging
import os
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import (
    broadcast,
    col,
    coalesce as spark_coalesce,
    count as spark_count,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    input_file_name,
    lit,
    lower,
    month,
    make_date,
    quarter,
    regexp_replace,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    upper,
    weekofyear,
    when,
    year,
    hash,
)
from pyspark.storagelevel import StorageLevel

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("transform")
STAGE_TOTAL_SECONDS = defaultdict(float)
STAGE_CALL_COUNT = defaultdict(int)


@contextmanager
def stage_timer(stage: str):
    start_ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    start = time.perf_counter()
    logger.info("[START] stage=%s ts=%s", stage, start_ts)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        STAGE_TOTAL_SECONDS[stage] += elapsed
        STAGE_CALL_COUNT[stage] += 1
        end_ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        logger.info("[END] stage=%s ts=%s elapsed_seconds=%.3f", stage, end_ts, elapsed)


def log_stage_summary() -> None:
    if not STAGE_TOTAL_SECONDS:
        return

    logger.info("========== Stage Runtime Summary ==========")
    for stage, total in sorted(STAGE_TOTAL_SECONDS.items(), key=lambda x: x[1], reverse=True):
        count = STAGE_CALL_COUNT[stage]
        avg = total / count if count else 0.0
        logger.info(
            "stage=%s total_seconds=%.3f calls=%s avg_seconds=%.3f",
            stage,
            total,
            count,
            avg,
        )


def env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        return default
    return value


def with_postgres_jdbc_optimizations(url: str) -> str:
    if not url.startswith("jdbc:postgresql://"):
        return url
    if "rewritebatchedinserts=" in url.lower():
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}reWriteBatchedInserts=true"


def build_spark() -> SparkSession:
    minio_endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = env("MINIO_SECRET_KEY", "12345678")

    return (
        SparkSession.builder.appName("nifi-minio-spark-realdata")
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.default.parallelism", env("SPARK_DEFAULT_PARALLELISM", "16"))
        .config("spark.sql.files.maxPartitionBytes", env("SPARK_FILES_MAX_PARTITION_BYTES", "134217728"))
        .config("spark.sql.files.openCostInBytes", env("SPARK_FILES_OPEN_COST_BYTES", "4194304"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", env("SPARK_ADVISORY_PARTITION_SIZE_BYTES", "134217728"))
        .config("spark.sql.autoBroadcastJoinThreshold", env("SPARK_AUTO_BROADCAST_JOIN_THRESHOLD", "104857600"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", env("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def with_target_partitions(df: DataFrame, target_partitions: int) -> DataFrame:
    safe_target = max(target_partitions, 1)
    return (
        df.coalesce(safe_target)
        if df.rdd.getNumPartitions() >= safe_target
        else df.repartition(safe_target)
    )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.parquet(path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )


def normalize_columns(df: DataFrame, important_cols: list[str] | None = None) -> DataFrame:
    string_cols = {c for c, t in df.dtypes if t == "string"}
    null_tokens = ("null", "none", "n/a", "na")
    target_cols = set(important_cols or [])
    exprs = []

    for c in df.columns:
        should_normalize = c in string_cols and (not target_cols or c in target_cols)
        if should_normalize:
            cleaned = trim(col(c))
            lowered = lower(cleaned)
            exprs.append(
                when(
                    cleaned.isNull() | (cleaned == "") | lowered.isin(*null_tokens),
                    None,
                ).otherwise(cleaned).alias(c)
            )
        else:
            exprs.append(col(c))

    # Keep normalization in a single projection so Catalyst can optimize it well.
    return df.select(*exprs)


def normalize_and_dedup(
    df: DataFrame,
    label: str,
    important_cols: list[str],
    key_candidates: list[str],
    num_partitions: int,
) -> DataFrame:
    normalized_cols = [c for c in important_cols if c in df.columns]
    logger.info("[%s] normalize target columns=%s", label, normalized_cols)

    # 1) Normalize string columns with one projection to avoid deep withColumn chains.
    out = normalize_columns(df, important_cols=important_cols)

    # 2) Drop rows that have no business data at all.
    business_cols = [c for c in out.columns if not c.startswith("_")]
    if business_cols:
        out = out.dropna(how="all", subset=business_cols)

    # 3) Deduplicate by best available business keys.
    keys = [k for k in key_candidates if k in out.columns]
    if not keys and business_cols:
        keys = business_cols
        logger.warning("[%s] no candidate keys found, fallback dedup by all business cols", label)

    if keys:
        out = out.dropDuplicates(keys)

    logger.info("[%s] normalize+dedup materialized keys=%s", label, keys)
    return out


def select_important_columns(df: DataFrame, label: str, important_cols: list[str]) -> DataFrame:
    meta_cols = ["_source_file", "_ingested_at"]
    selected_cols: list[str] = []

    for c in important_cols + meta_cols:
        if c in df.columns and c not in selected_cols:
            selected_cols.append(c)

    missing_cols = [c for c in important_cols if c not in df.columns]
    if missing_cols:
        logger.warning("[%s] important columns missing in input=%s", label, missing_cols)

    logger.info("[%s] selected columns=%s", label, selected_cols)
    return df.select(*selected_cols)


def write_staging_table(
    df: DataFrame,
    table_name: str,
    pg_url: str,
    jdbc_props: dict,
    mode: str,
    batch_size: str,
    num_partitions: int,
) -> None:
    logger.info("Writing table=%s mode=%s requested_partitions=%s", table_name, mode, num_partitions)
    target_partitions = max(num_partitions, 1)
    logger.info("Applying target_partitions=%s before JDBC write", target_partitions)

    (
        with_target_partitions(df, target_partitions)
        .write.mode(mode)
        .option("batchsize", batch_size)
        .option("numPartitions", str(target_partitions))
        .option("isolationLevel", "NONE")
        .jdbc(pg_url, table_name, properties=jdbc_props)
    )


def write_parquet_optimized(df: DataFrame, path: str, mode: str, num_partitions: int) -> None:
    """Write Parquet with snappy compression using a configurable target partition count."""
    target_partitions = max(num_partitions, 1)
    logger.info("Writing parquet path=%s mode=%s requested_partitions=%s target_partitions=%s", path, mode, num_partitions, target_partitions)
    (
        with_target_partitions(df, target_partitions)
        .write
        .mode(mode)
        .option("compression", "snappy")
        .parquet(path)
    )


def read_jdbc_table(spark: SparkSession, table_name: str, pg_url: str, jdbc_props: dict) -> DataFrame:
    """Read a JDBC table using options that avoid collecting the full table into driver memory.

    Uses the DataFrameReader .format("jdbc") with fetchsize so partitions are streamed from the
    database instead of loading the entire table into memory.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", table_name)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .option("driver", jdbc_props["driver"])
        .option("fetchsize", "10000")
        .load()
    )


def maybe_materialize(df: DataFrame, label: str, enabled: bool) -> DataFrame:
    if not enabled:
        logger.info("[%s] skip materialize (set PIPELINE_PROFILE_MATERIALIZE=true to enable)", label)
        return df

    with stage_timer(f"{label}.materialize_count"):
        count = df.count()
        logger.info("[%s] row_count=%s", label, count)
    return df


def to_s3a_path(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def iter_chunks(values: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for i in range(0, len(values), chunk_size):
        yield values[i : i + chunk_size]


def read_dim_call_type_subset(
    spark: SparkSession,
    pg_url: str,
    jdbc_props: dict,
    call_type_codes: list[str],
) -> DataFrame:
    if not call_type_codes:
        return spark.createDataFrame([], "call_type_key long, call_type_code string")

    subset_df: DataFrame | None = None
    for chunk in iter_chunks(call_type_codes, 500):
        in_clause = ",".join(f"'{escape_sql_literal(str(code))}'" for code in chunk)
        query = f"""(
            SELECT call_type_key, call_type_code
            FROM dwh.dim_call_type
            WHERE call_type_code IN ({in_clause})
        ) dim_call_type_subset"""
        chunk_df = read_jdbc_table(spark, query, pg_url, jdbc_props).select("call_type_key", "call_type_code")
        subset_df = chunk_df if subset_df is None else subset_df.unionByName(chunk_df)

    if subset_df is None:
        return spark.createDataFrame([], "call_type_key long, call_type_code string")
    return subset_df.dropDuplicates(["call_type_code"])


def execute_jdbc_update(spark: SparkSession, pg_url: str, jdbc_props: dict, sql: str) -> None:
    conn = None
    stmt = None
    try:
        jvm = spark._sc._jvm
        jvm.java.lang.Class.forName(jdbc_props["driver"])
        conn = jvm.java.sql.DriverManager.getConnection(pg_url, jdbc_props["user"], jdbc_props["password"])
        stmt = conn.createStatement()
        stmt.executeUpdate(sql)
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def update_manifest_status(
    spark: SparkSession,
    pg_url: str,
    jdbc_props: dict,
    ids: list[int],
    batch_id: str,
    status: int,
    error_message: str | None = None,
) -> None:
    if not ids:
        return

    err_sql = "NULL"
    if error_message:
        err_sql = f"'{escape_sql_literal(error_message[:1900])}'"

    for chunk in iter_chunks(ids, 500):
        id_clause = ",".join(str(i) for i in chunk)
        sql = f"""
        UPDATE public.ingest_manifest
        SET processed_flag = {status},
            processed_time = CURRENT_TIMESTAMP,
            error_message = {err_sql}
        WHERE id IN ({id_clause})
          AND batch_id = '{escape_sql_literal(batch_id)}'
        """
        execute_jdbc_update(spark, pg_url, jdbc_props, sql)


def claim_manifest_batch(
    spark: SparkSession,
    pg_url: str,
    jdbc_props: dict,
    job_name: str,
    batch_id: str,
    batch_size: int,
) -> DataFrame:
    pending_query = f"""(
        SELECT id
        FROM public.ingest_manifest
        WHERE job_name = '{escape_sql_literal(job_name)}'
          AND processed_flag = 0
        ORDER BY ingest_time ASC, id ASC
        LIMIT {batch_size}
    ) t"""
    pending_ids = [int(r["id"]) for r in read_jdbc_table(spark, pending_query, pg_url, jdbc_props).toLocalIterator()]
    if not pending_ids:
        return spark.createDataFrame([], "id long, s3_path string")

    for chunk in iter_chunks(pending_ids, 500):
        id_clause = ",".join(str(i) for i in chunk)
        claim_sql = f"""
        UPDATE public.ingest_manifest
        SET processed_flag = 9,
            batch_id = '{escape_sql_literal(batch_id)}',
            error_message = NULL
        WHERE id IN ({id_clause})
          AND processed_flag = 0
        """
        execute_jdbc_update(spark, pg_url, jdbc_props, claim_sql)

    claimed_query = f"""(
        SELECT id, s3_path
        FROM public.ingest_manifest
        WHERE job_name = '{escape_sql_literal(job_name)}'
          AND processed_flag = 9
          AND batch_id = '{escape_sql_literal(batch_id)}'
        ORDER BY ingest_time ASC, id ASC
    ) claimed"""
    return read_jdbc_table(spark, claimed_query, pg_url, jdbc_props)


def build_csv_schema_from_header(
    spark: SparkSession,
    sample_path: str,
    type_overrides: dict[str, object] | None = None,
) -> StructType:
    overrides = type_overrides or {}
    header_row = spark.read.text(sample_path).head()
    if not header_row:
        raise ValueError(f"Cannot build CSV schema: file has no header row: {sample_path}")

    raw_header = (header_row[0] or "").lstrip("\ufeff")
    header_cols = [c.strip() for c in raw_header.split(",") if c and c.strip()]
    if not header_cols:
        raise ValueError(f"Cannot build CSV schema: empty header row in file: {sample_path}")

    fields = [StructField(c, overrides.get(c, StringType()), True) for c in header_cols]
    return StructType(fields)


def read_manifest_csv_with_schema(
    spark: SparkSession,
    paths: list[str],
    schema: StructType,
) -> DataFrame:
    if not paths:
        return spark.createDataFrame([], "_source_file string, _ingested_at timestamp")
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "DROPMALFORMED")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(schema)
        .csv(paths)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )


def build_empty_raw_df(spark: SparkSession, important_cols: list[str]) -> DataFrame:
    fields = [StructField(c, StringType(), True) for c in important_cols]
    fields.extend(
        [
            StructField("_source_file", StringType(), True),
            StructField("_ingested_at", TimestampType(), True),
        ]
    )
    return spark.createDataFrame([], StructType(fields))


def main() -> None:
    with stage_timer("spark_job.total"):
        logger.info("Starting Spark job for real Parquet sources")
        with stage_timer("spark_session.build"):
            spark = build_spark()

        bucket = env("DATALAKE_BUCKET", "datalake")
        raw_base = f"s3a://{bucket}/raw"
        processed_base = f"s3a://{bucket}/processed"
        curated_base = f"s3a://{bucket}/curated"

        minio_write_mode = env("MINIO_WRITE_MODE", "overwrite")
        stg_write_mode = env("STG_WRITE_MODE", "append")
        dwh_write_mode = env("DWH_FACT_WRITE_MODE", "append")
        shuffle_partitions = int(env("SPARK_SHUFFLE_PARTITIONS", "16"))
        jdbc_batch_size = env("JDBC_BATCH_SIZE", "10000")
        jdbc_num_partitions = int(env("JDBC_NUM_PARTITIONS", "12"))
        manifest_job_name = env("INGEST_MANIFEST_JOB_NAME", "transform_usage_daily")
        manifest_batch_size = int(env("INGEST_MANIFEST_BATCH_SIZE", "100"))
        curated_output_partitions = int(env("CURATED_FACT_OUTPUT_PARTITIONS", "32"))
        processed_output_partitions = int(env("PROCESSED_OUTPUT_PARTITIONS", "50"))
        enable_salt_agg = env("ENABLE_SALT_AGG", "false").lower() == "true"
        salt_buckets = int(env("SALT_BUCKETS", "8"))
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        pg_url = with_postgres_jdbc_optimizations(
            env("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres")
        )
        pg_user = env("PG_USER", "postgres")
        pg_pass = env("PG_PASSWORD", "123456")
        jdbc_props = {"user": pg_user, "password": pg_pass, "driver": "org.postgresql.Driver"}

        profile_materialize = env("PIPELINE_PROFILE_MATERIALIZE", "false").lower() == "true"

        with stage_timer("manifest.claim"):
            df_manifest = claim_manifest_batch(
                spark=spark,
                pg_url=pg_url,
                jdbc_props=jdbc_props,
                job_name=manifest_job_name,
                batch_id=batch_id,
                batch_size=manifest_batch_size,
            )
        manifest_sample = df_manifest.take(1)
        claimed_ids: list[int] = []

        if not manifest_sample:
            logger.info("No pending rows in ingest_manifest for job=%s. Nothing to process.", manifest_job_name)
            with stage_timer("spark_session.stop"):
                spark.stop()
            return

        flexi_paths: list[str] = []
        icc_paths: list[str] = []
        unknown_ids: list[int] = []
        logger.info("Claimed rows from manifest batch_id=%s", batch_id)
        for row in df_manifest.toLocalIterator():
            row_id = int(row["id"])
            s3_path = str(row["s3_path"])
            claimed_ids.append(row_id)
            normalized_path = to_s3a_path(s3_path)
            lowered = normalized_path.lower()
            if "frt_flexi_export_" in lowered:
                flexi_paths.append(normalized_path)
            elif "frt_in_icc_export_" in lowered or "in_icc" in lowered:
                icc_paths.append(normalized_path)
            else:
                unknown_ids.append(row_id)

        if unknown_ids:
            update_manifest_status(
                spark,
                pg_url,
                jdbc_props,
                unknown_ids,
                batch_id=batch_id,
                status=2,
                error_message="Unsupported filename pattern. Expected flexi/icc export naming.",
            )

        try:
            flexi_important_cols = [
                "charging_id",
                "record_sequence_number",
                "record_opening_time",
                "record_type",
                "duration",
                "served_msisdn",
                "ftp_filename",
            ]
            icc_important_cols = [
                "org_call_id",
                "call_reference",
                "call_sta_time",
                "call_type",
                "used_duration",
            ]

            if flexi_paths:
                logger.info("Reading flexi CSV from manifest paths count=%s", len(flexi_paths))
                with stage_timer("read_csv.flexi"):
                    flexi_schema = build_csv_schema_from_header(spark, flexi_paths[0])
                    df_flexi_raw = read_manifest_csv_with_schema(spark, flexi_paths, flexi_schema)
            else:
                logger.info("No flexi paths in this manifest batch. Using empty input for flexi branch.")
                df_flexi_raw = build_empty_raw_df(spark, flexi_important_cols)

            if icc_paths:
                logger.info("Reading icc CSV from manifest paths count=%s", len(icc_paths))
                with stage_timer("read_csv.icc"):
                    icc_schema = build_csv_schema_from_header(
                        spark,
                        icc_paths[0],
                        type_overrides={"used_duration": DoubleType()},
                    )
                    df_icc_raw = read_manifest_csv_with_schema(spark, icc_paths, icc_schema)
            else:
                logger.info("No icc paths in this manifest batch. Using empty input for icc branch.")
                df_icc_raw = build_empty_raw_df(spark, icc_important_cols)

            with stage_timer("select_important_columns.flexi"):
                df_flexi_raw = select_important_columns(df_flexi_raw, "flexi", flexi_important_cols)
            with stage_timer("select_important_columns.icc"):
                df_icc_raw = select_important_columns(df_icc_raw, "icc", icc_important_cols)

            with stage_timer("normalize_and_dedup.flexi"):
                df_flexi = normalize_and_dedup(
                    df_flexi_raw,
                    "flexi",
                    flexi_important_cols,
                    [
                        "charging_id",
                        "record_sequence_number",
                        "record_opening_time",
                        "served_msisdn",
                        "ftp_filename",
                    ],
                    shuffle_partitions,
                )
            with stage_timer("normalize_and_dedup.icc"):
                df_icc = normalize_and_dedup(
                    df_icc_raw,
                    "icc",
                    icc_important_cols,
                    [
                        "org_call_id",
                        "call_reference",
                    ],
                    shuffle_partitions,
                )

            df_flexi = maybe_materialize(df_flexi, "flexi", profile_materialize)
            df_icc = maybe_materialize(df_icc, "icc", profile_materialize)

            # Data lake write (bronze/processed style)
            logger.info("Writing processed parquet zones")
            with stage_timer("write_parquet.processed.flexi"):
                write_parquet_optimized(
                    df_flexi,
                    f"{processed_base}/frt_flexi_raw/",
                    minio_write_mode,
                    processed_output_partitions,
                )
            with stage_timer("write_parquet.processed.icc"):
                write_parquet_optimized(
                    df_icc,
                    f"{processed_base}/frt_in_icc_raw/",
                    minio_write_mode,
                    processed_output_partitions,
                )

            # PostgreSQL staging write (full raw structure)
            logger.info("Writing staging tables in PostgreSQL")
            flexi_usage_date = spark_coalesce(
                to_date(col("record_opening_time"), "yyyy-MM-dd"),
                to_date(col("record_opening_time"), "yyyyMMdd"),
                to_date(to_timestamp(col("record_opening_time"), "yyyy-MM-dd HH:mm:ss")),
                to_date(to_timestamp(col("record_opening_time"), "yyyyMMddHHmmss")),
                to_date(to_timestamp(col("record_opening_time"), "dd/MM/yyyy HH:mm:ss")),
                to_date(
                    to_timestamp(
                        regexp_replace(trim(col("record_opening_time")), r"^[A-Za-z]{3}\s+", ""),
                        "MMM dd HH:mm:ss yyyy",
                    )
                ),
            )
            df_flexi_pg = (
                df_flexi
                .withColumn("usage_date_tmp", flexi_usage_date)
                .withColumn("year", year(col("usage_date_tmp")).cast("int"))
                .withColumn("month", month(col("usage_date_tmp")).cast("int"))
                .withColumn("day", dayofmonth(col("usage_date_tmp")).cast("int"))
                .drop("usage_date_tmp")
            )

            icc_usage_date = spark_coalesce(
                to_date(to_timestamp(col("call_sta_time"), "dd/MM/yyyy HH:mm:ss")),
                to_date(col("call_sta_time"), "yyyy-MM-dd"),
                to_date(col("call_sta_time"), "yyyyMMdd"),
            )
            df_icc_pg = (
                df_icc
                .withColumn("usage_date_tmp", icc_usage_date)
                .withColumn("year", year(col("usage_date_tmp")).cast("int"))
                .withColumn("month", month(col("usage_date_tmp")).cast("int"))
                .withColumn("day", dayofmonth(col("usage_date_tmp")).cast("int"))
                .drop("usage_date_tmp")
            )

            with stage_timer("write_jdbc.stg_frt_flexi_raw"):
                write_staging_table(
                    df_flexi_pg,
                    "public.stg_frt_flexi_raw",
                    pg_url,
                    jdbc_props,
                    stg_write_mode,
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )
            with stage_timer("write_jdbc.stg_frt_in_icc_raw"):
                write_staging_table(
                    df_icc_pg,
                    "public.stg_frt_in_icc_raw",
                    pg_url,
                    jdbc_props,
                    stg_write_mode,
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )

            # Build curated mart and load star schema dimensions.
            logger.info("Building curated daily usage mart from Flexi")
            with stage_timer("build_curated.flexi_daily"):
                df_flexi_prepared = (
                    df_flexi_pg.select("charging_id", "record_type", "duration", "year", "month", "day")
                    .withColumn("usage_date", make_date(col("year"), col("month"), col("day")))
                    .withColumn(
                        "call_type_code",
                        when(
                            col("record_type").isNull() | (trim(col("record_type")) == ""),
                            lit("UNKNOWN"),
                        ).otherwise(upper(trim(col("record_type")))),
                    )
                    .withColumn("used_duration_num", col("duration").cast("double"))
                    .where(col("usage_date").isNotNull())
                )

                if enable_salt_agg:
                    logger.info("Using salted 2-phase aggregation for skew mitigation: buckets=%s", salt_buckets)
                    df_flexi_daily = (
                        df_flexi_prepared
                        .withColumn("salt", (hash(col("charging_id")) % lit(salt_buckets)).cast("int"))
                        .groupBy("usage_date", "call_type_code", "salt")
                        .agg(
                            spark_count("*").alias("event_count"),
                            spark_sum("used_duration_num").alias("total_used_duration"),
                        )
                        .groupBy("usage_date", "call_type_code")
                        .agg(
                            spark_sum("event_count").alias("event_count"),
                            spark_sum("total_used_duration").alias("total_used_duration"),
                        )
                    )
                else:
                    logger.info("Using single-phase aggregation to avoid extra shuffle from salting")
                    df_flexi_daily = (
                        df_flexi_prepared
                        .groupBy("usage_date", "call_type_code")
                        .agg(
                            spark_count("*").alias("event_count"),
                            spark_sum("used_duration_num").alias("total_used_duration"),
                        )
                    )

            df_flexi_daily = df_flexi_daily.persist(StorageLevel.MEMORY_AND_DISK)
            df_flexi_daily = maybe_materialize(df_flexi_daily, "flexi_daily", profile_materialize)

            logger.info("Building and loading dwh.dim_date")
            with stage_timer("build_dim.dim_date"):
                df_dim_date_candidates = (
                    df_flexi_daily.select("usage_date")
                    .dropna(subset=["usage_date"])
                    .dropDuplicates(["usage_date"])
                    .withColumn("date_key", date_format(col("usage_date"), "yyyyMMdd").cast("int"))
                    .withColumn("full_date", col("usage_date"))
                    .withColumn("day_of_month", dayofmonth(col("usage_date")).cast("smallint"))
                    .withColumn("month_of_year", month(col("usage_date")).cast("smallint"))
                    .withColumn("quarter_of_year", quarter(col("usage_date")).cast("smallint"))
                    .withColumn("year_number", year(col("usage_date")).cast("int"))
                    .withColumn("week_of_year", weekofyear(col("usage_date")).cast("smallint"))
                    .withColumn("day_name", date_format(col("usage_date"), "E"))
                    .withColumn("month_name", date_format(col("usage_date"), "MMM"))
                    .withColumn(
                        "is_weekend",
                        when(dayofweek(col("usage_date")).isin(1, 7), lit(True)).otherwise(lit(False)),
                    )
                    .select(
                        "date_key",
                        "full_date",
                        "day_of_month",
                        "month_of_year",
                        "quarter_of_year",
                        "year_number",
                        "week_of_year",
                        "day_name",
                        "month_name",
                        "is_weekend",
                    )
                )

            with stage_timer("write_jdbc.dwh_dim_date"):
                df_dim_date_existing = read_jdbc_table(spark, "dwh.dim_date", pg_url, jdbc_props).select("date_key")
                df_dim_date_new = df_dim_date_candidates.join(df_dim_date_existing, "date_key", "left_anti")
                write_staging_table(
                    df_dim_date_new,
                    "dwh.dim_date",
                    pg_url,
                    jdbc_props,
                    "append",
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )

            logger.info("Building and loading dwh.dim_call_type")
            with stage_timer("build_dim.dim_call_type"):
                df_dim_call_type_candidates = (
                    df_flexi_daily.select("call_type_code")
                    .dropna(subset=["call_type_code"])
                    .dropDuplicates(["call_type_code"])
                    .withColumn("call_type_name", col("call_type_code"))
                    .withColumn("is_active", lit(True))
                    .select("call_type_code", "call_type_name", "is_active")
                )

            with stage_timer("write_jdbc.dwh_dim_call_type"):
                df_call_type_codes = df_dim_call_type_candidates.select("call_type_code").dropDuplicates(["call_type_code"])
                df_dim_call_type_existing = (
                    read_jdbc_table(spark, "dwh.dim_call_type", pg_url, jdbc_props)
                    .select("call_type_code")
                    .repartition(max(1, min(jdbc_num_partitions, 32)), "call_type_code")
                    .join(df_call_type_codes, "call_type_code", "inner")
                    .dropDuplicates(["call_type_code"])
                )
                df_dim_call_type_new = df_dim_call_type_candidates.join(
                    df_dim_call_type_existing, "call_type_code", "left_anti"
                )
                write_staging_table(
                    df_dim_call_type_new,
                    "dwh.dim_call_type",
                    pg_url,
                    jdbc_props,
                    "append",
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )

            logger.info("Joining dimensions and building key-based fact")
            with stage_timer("build_fact.flexi_daily_keys"):
                df_fact_call_type_codes = (
                    df_flexi_daily.select("call_type_code").dropDuplicates(["call_type_code"]).persist(StorageLevel.MEMORY_AND_DISK)
                )
                df_dim_call_type = (
                    read_jdbc_table(spark, "dwh.dim_call_type", pg_url, jdbc_props)
                    .select("call_type_key", "call_type_code")
                    .repartition(max(1, min(jdbc_num_partitions, 32)), "call_type_code")
                    .join(df_fact_call_type_codes, "call_type_code", "inner")
                    .dropDuplicates(["call_type_code"])
                )
                # dim_call_type is expected to be small; broadcast unconditionally
                # to avoid an extra count() action on df_fact_call_type_codes.
                df_dim_call_type = broadcast(df_dim_call_type)

                df_fact_usage_daily = (
                    df_flexi_daily.withColumn("date_key", date_format(col("usage_date"), "yyyyMMdd").cast("int"))
                    .join(df_dim_call_type, "call_type_code", "left")
                    .withColumn("call_type_key", col("call_type_key").cast("long"))
                    .withColumn("total_used_duration", spark_coalesce(col("total_used_duration"), lit(0.0)))
                    .select("date_key", "call_type_key", "call_type_code", "event_count", "total_used_duration")
                    .where(col("date_key").isNotNull() & col("call_type_key").isNotNull())
                )

                df_fact_usage_daily_curated = (
                    df_fact_usage_daily
                    .withColumn("usage_date", to_date(col("date_key").cast("string"), "yyyyMMdd"))
                    .select(
                        "date_key",
                        "call_type_key",
                        "usage_date",
                        "call_type_code",
                        "event_count",
                        "total_used_duration",
                    )
                )
                df_fact_call_type_codes.unpersist()

            with stage_timer("write_parquet.curated.fact_usage_daily"):
                write_parquet_optimized(
                    df_fact_usage_daily_curated,
                    f"{curated_base}/fact_usage_daily/",
                    minio_write_mode,
                    curated_output_partitions,
                )

            with stage_timer("write_jdbc.dwh_fact_usage_daily"):
                write_staging_table(
                    df_fact_usage_daily_curated.select(
                        "date_key",
                        "call_type_key",
                        "usage_date",
                        col("call_type_code").alias("call_type"),
                        "event_count",
                        "total_used_duration",
                    ),
                    "dwh.fact_usage_daily",
                    pg_url,
                    jdbc_props,
                    dwh_write_mode,
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )

            logger.info("Building and loading dwh.usage_summary_daily")
            with stage_timer("build_summary.usage_daily"):
                df_usage_summary_daily_curated = (
                    df_fact_usage_daily_curated
                    .groupBy("usage_date", "call_type_key", "call_type_code")
                    .agg(
                        spark_sum("event_count").alias("event_count"),
                        spark_sum("total_used_duration").alias("total_used_duration"),
                    )
                    .select(
                        "usage_date",
                        "call_type_key",
                        "call_type_code",
                        "event_count",
                        "total_used_duration",
                    )
                )

            with stage_timer("write_jdbc.dwh_usage_summary_daily"):
                summary_usage_dates = [
                    r["usage_date"]
                    for r in df_usage_summary_daily_curated.select("usage_date").dropDuplicates(["usage_date"]).toLocalIterator()
                ]
                for chunk in iter_chunks(summary_usage_dates, 500):
                    date_clause = ",".join(f"DATE '{d}'" for d in chunk)
                    delete_sql = f"DELETE FROM dwh.usage_summary_daily WHERE usage_date IN ({date_clause})"
                    execute_jdbc_update(spark, pg_url, jdbc_props, delete_sql)

                write_staging_table(
                    df_usage_summary_daily_curated,
                    "dwh.usage_summary_daily",
                    pg_url,
                    jdbc_props,
                    "append",
                    jdbc_batch_size,
                    jdbc_num_partitions,
                )

            df_flexi_daily.unpersist()
            if df_flexi.is_cached:
                df_flexi.unpersist()
            if df_icc.is_cached:
                df_icc.unpersist()

            success_ids = [i for i in claimed_ids if i not in unknown_ids]
            update_manifest_status(
                spark,
                pg_url,
                jdbc_props,
                success_ids,
                batch_id=batch_id,
                status=1,
            )
        except Exception as exc:
            error_text = str(exc)
            failed_ids = [i for i in claimed_ids if i not in unknown_ids]
            update_manifest_status(
                spark,
                pg_url,
                jdbc_props,
                failed_ids,
                batch_id=batch_id,
                status=2,
                error_message=error_text,
            )
            raise

        with stage_timer("spark_session.stop"):
            spark.stop()
        logger.info("Spark job completed successfully")



if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Spark job failed")
        raise
