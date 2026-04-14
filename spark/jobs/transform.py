import logging
import os
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
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
    quarter,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    upper,
    weekofyear,
    when,
    year,
)

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


def build_spark() -> SparkSession:
    minio_endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = env("MINIO_SECRET_KEY", "12345678")

    return (
        SparkSession.builder.appName("nifi-minio-spark-realdata")
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
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
    logger.info("Writing table=%s mode=%s partitions=%s", table_name, mode, num_partitions)

    out_df = df.coalesce(num_partitions)
    (
        out_df.write.mode(mode)
        .option("batchsize", batch_size)
        .option("numPartitions", str(num_partitions))
        .jdbc(pg_url, table_name, properties=jdbc_props)
    )


def read_jdbc_table(spark: SparkSession, table_name: str, pg_url: str, jdbc_props: dict) -> DataFrame:
    return spark.read.jdbc(pg_url, table_name, properties=jdbc_props)


def maybe_materialize(df: DataFrame, label: str, enabled: bool) -> DataFrame:
    if not enabled:
        logger.info("[%s] skip materialize (set PIPELINE_PROFILE_MATERIALIZE=true to enable)", label)
        return df

    with stage_timer(f"{label}.materialize_count"):
        count = df.count()
        logger.info("[%s] row_count=%s", label, count)
    return df


def main() -> None:
    with stage_timer("spark_job.total"):
        logger.info("Starting Spark job for real Parquet sources")
        with stage_timer("spark_session.build"):
            spark = build_spark()

        bucket = env("DATALAKE_BUCKET", "datalake")
        raw_base = f"s3a://{bucket}/raw"
        processed_base = f"s3a://{bucket}/processed"
        curated_base = f"s3a://{bucket}/curated"

        # Raw objects are stored as parquet under raw/.
        flexi_path = env("RAW_FLEXI_PATH", f"{raw_base}/frt_flexi_export_*/part_*.parquet")
        icc_path = env("RAW_ICC_PATH", f"{raw_base}/frt_in_icc_export_*/part_*.parquet")

        minio_write_mode = env("MINIO_WRITE_MODE", "overwrite")
        stg_write_mode = env("STG_WRITE_MODE", "overwrite")
        dwh_write_mode = env("DWH_FACT_WRITE_MODE", "append")
        shuffle_partitions = int(env("SPARK_SHUFFLE_PARTITIONS", "8"))
        jdbc_batch_size = env("JDBC_BATCH_SIZE", "10000")
        jdbc_num_partitions = int(env("JDBC_NUM_PARTITIONS", "8"))

        pg_url = env("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres")
        pg_user = env("PG_USER", "postgres")
        pg_pass = env("PG_PASSWORD", "123456")
        jdbc_props = {"user": pg_user, "password": pg_pass, "driver": "org.postgresql.Driver"}

        profile_materialize = env("PIPELINE_PROFILE_MATERIALIZE", "false").lower() == "true"

        logger.info("Reading flexi Parquet from %s", flexi_path)
        with stage_timer("read_parquet.flexi"):
            df_flexi_raw = read_parquet(spark, flexi_path)
        logger.info("Reading icc Parquet from %s", icc_path)
        with stage_timer("read_parquet.icc"):
            df_icc_raw = read_parquet(spark, icc_path)

        flexi_important_cols = [
            "org_call_id",
            "charging_id",
            "record_sequence_number",
            "record_opening_time",
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
                    "org_call_id",
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
            df_flexi.coalesce(4).write.mode(minio_write_mode).parquet(f"{processed_base}/frt_flexi_raw/")
        with stage_timer("write_parquet.processed.icc"):
            df_icc.coalesce(4).write.mode(minio_write_mode).parquet(f"{processed_base}/frt_in_icc_raw/")

        # PostgreSQL staging write (full raw structure)
        logger.info("Writing staging tables in PostgreSQL")
        with stage_timer("write_jdbc.stg_frt_flexi_raw"):
            write_staging_table(
                df_flexi,
                "public.stg_frt_flexi_raw",
                pg_url,
                jdbc_props,
                stg_write_mode,
                jdbc_batch_size,
                jdbc_num_partitions,
            )
        with stage_timer("write_jdbc.stg_frt_in_icc_raw"):
            write_staging_table(
                df_icc,
                "public.stg_frt_in_icc_raw",
                pg_url,
                jdbc_props,
                stg_write_mode,
                jdbc_batch_size,
                jdbc_num_partitions,
            )

        # Build curated mart and load star schema dimensions.
        logger.info("Building curated daily usage mart from ICC")
        with stage_timer("build_curated.icc_daily"):
            df_icc_small = df_icc.select("call_sta_time", "call_type", "used_duration")
            df_icc_daily = (
                df_icc_small.withColumn(
                    "call_start_ts",
                    to_timestamp(col("call_sta_time"), "dd/MM/yyyy HH:mm:ss"),
                )
                .withColumn("usage_date", to_date(col("call_start_ts")))
                .withColumn(
                    "call_type_code",
                    when(
                        col("call_type").isNull() | (trim(col("call_type")) == ""),
                        lit("UNKNOWN"),
                    ).otherwise(upper(trim(col("call_type")))),
                )
                .withColumn("used_duration_num", col("used_duration").cast("double"))
                .where(col("usage_date").isNotNull())
                .groupBy("usage_date", "call_type_code")
                .agg(
                    spark_count("*").alias("event_count"),
                    spark_sum("used_duration_num").alias("total_used_duration"),
                )
            )

        df_icc_daily = maybe_materialize(df_icc_daily, "icc_daily", profile_materialize)

        logger.info("Building and loading dwh.dim_date")
        with stage_timer("build_dim.dim_date"):
            df_dim_date_candidates = (
                df_icc_daily.select("usage_date")
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
                df_icc_daily.select("call_type_code")
                .dropna(subset=["call_type_code"])
                .dropDuplicates(["call_type_code"])
                .withColumn("call_type_name", col("call_type_code"))
                .withColumn("is_active", lit(True))
                .select("call_type_code", "call_type_name", "is_active")
            )

        with stage_timer("write_jdbc.dwh_dim_call_type"):
            df_dim_call_type_existing = read_jdbc_table(
                spark, "dwh.dim_call_type", pg_url, jdbc_props
            ).select("call_type_code")
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
        with stage_timer("build_fact.icc_daily_keys"):
            df_dim_date = read_jdbc_table(spark, "dwh.dim_date", pg_url, jdbc_props).select(
                "date_key", "full_date"
            )
            df_dim_call_type = read_jdbc_table(spark, "dwh.dim_call_type", pg_url, jdbc_props).select(
                "call_type_key", "call_type_code"
            )

            df_fact_usage_daily = (
                df_icc_daily.withColumn("date_key", date_format(col("usage_date"), "yyyyMMdd").cast("int"))
                .join(df_dim_call_type, "call_type_code", "left")
                .withColumn("call_type_key", col("call_type_key").cast("long"))
                .withColumn("total_used_duration", spark_coalesce(col("total_used_duration"), lit(0.0)))
                .select("date_key", "call_type_key", "event_count", "total_used_duration")
                .where(col("date_key").isNotNull() & col("call_type_key").isNotNull())
            )

            # Keep curated parquet human-friendly while fact JDBC uses surrogate keys.
            df_fact_usage_daily_curated = (
                df_fact_usage_daily.join(df_dim_date, "date_key", "left")
                .join(df_dim_call_type, "call_type_key", "left")
                .select(
                    "date_key",
                    "call_type_key",
                    col("full_date").alias("usage_date"),
                    "call_type_code",
                    "event_count",
                    "total_used_duration",
                )
            )

        with stage_timer("write_parquet.curated.fact_usage_daily"):
            df_fact_usage_daily_curated.coalesce(4).write.mode(minio_write_mode).parquet(
                f"{curated_base}/fact_usage_daily/"
            )

        with stage_timer("write_jdbc.dwh_fact_usage_daily"):
            write_staging_table(
                df_fact_usage_daily,
                "dwh.fact_usage_daily",
                pg_url,
                jdbc_props,
                dwh_write_mode,
                jdbc_batch_size,
                jdbc_num_partitions,
            )

        with stage_timer("spark_session.stop"):
            spark.stop()
        logger.info("Spark job completed successfully")



if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Spark job failed")
        raise
