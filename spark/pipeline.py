from __future__ import annotations

import logging
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce as spark_coalesce,
    col,
    current_timestamp,
    dayofmonth,
    input_file_name,
    lit,
    lower,
    month,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from spark.core import read_jdbc_table
from spark.utils import stage_timer

logger = logging.getLogger("transform")
NULL_TOKENS = ("null", "none", "n/a", "na")
NUMERIC_PATTERN = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$"


def normalize_columns(df: DataFrame, important_cols: list[str] | None = None) -> DataFrame:
    """Normalize only string columns that are in important_cols.

    Original code iterated every column and applied when/lower/trim even to
    columns that would be discarded immediately after.  By restricting to
    important_cols we skip metadata columns (_source_file, etc.) and extra
    CSV columns that will be dropped anyway.
    """
    string_cols = {c for c, t in df.dtypes if t == "string"}
    target_set = set(important_cols) if important_cols else string_cols
    cols_to_normalize = string_cols & target_set & set(df.columns)

    exprs = []
    for c in df.columns:
        if c in cols_to_normalize:
            cleaned = trim(col(c))
            exprs.append(
                when(cleaned.isNull() | (cleaned == "") | lower(cleaned).isin(*NULL_TOKENS), None)
                .otherwise(cleaned)
                .alias(c)
            )
        else:
            exprs.append(col(c))
    return df.select(*exprs)


def parse_nullable_double(column_name: str):
    raw = trim(col(column_name).cast("string"))
    lowered = lower(raw)
    return (
        when(raw.isNull() | (raw == "") | lowered.isin(*NULL_TOKENS), None)
        .when(raw.rlike(NUMERIC_PATTERN), raw.cast("double"))
        .otherwise(None)
    )


def normalize_and_dedup(
    df: DataFrame,
    label: str,
    important_cols: list[str],
    key_candidates: list[str],
    num_partitions: int,
) -> DataFrame:
    normalized_cols = [c for c in important_cols if c in df.columns]
    logger.info("[%s] normalize target columns=%s", label, normalized_cols)
    out = normalize_columns(df, important_cols=important_cols)
    business_cols = [c for c in out.columns if not c.startswith("_")]
    if business_cols:
        out = out.dropna(how="all", subset=business_cols)
    keys = [k for k in key_candidates if k in out.columns]
    if not keys and business_cols:
        keys = business_cols
        logger.warning("[%s] no candidate keys found, fallback dedup by all business cols", label)
    if keys:
        out = out.dropDuplicates(keys)
    logger.info("[%s] normalize+dedup keys=%s", label, keys)
    return out.repartition(max(num_partitions, 1))


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


def maybe_materialize(df: DataFrame, label: str, enabled: bool) -> DataFrame:
    if not enabled:
        logger.info("[%s] skip materialize (set PIPELINE_PROFILE_MATERIALIZE=true to enable)", label)
        return df
    with stage_timer(f"{label}.materialize_count"):
        count = df.count()
        logger.info("[%s] row_count=%s", label, count)
    return df


def has_any_row(df: DataFrame, label: str) -> bool:
    """Check if a DataFrame has at least one row without triggering a full scan.

    rdd.isEmpty() scans the entire dataset (equivalent to count() == 0).
    limit(1).count() stops as soon as it finds one row — O(1) happy path.
    """
    has_row = df.limit(1).count() > 0
    logger.info("[%s] has_any_row=%s", label, has_row)
    return has_row


def to_s3a_path(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def iter_chunks(values: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for i in range(0, len(values), chunk_size):
        yield values[i: i + chunk_size]


def _get_jdbc_connection(spark: SparkSession, pg_url: str, jdbc_props: dict):
    """Create a raw JDBC connection via the JVM gateway."""
    jvm = spark._sc._jvm
    jvm.java.lang.Class.forName(jdbc_props["driver"])
    return jvm.java.sql.DriverManager.getConnection(pg_url, jdbc_props["user"], jdbc_props["password"])


def execute_jdbc_update(spark: SparkSession, pg_url: str, jdbc_props: dict, sql: str) -> None:
    """Execute a single DML statement. Opens and closes its own connection."""
    conn = None
    stmt = None
    try:
        conn = _get_jdbc_connection(spark, pg_url, jdbc_props)
        stmt = conn.createStatement()
        stmt.executeUpdate(sql)
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def execute_jdbc_updates_batch(
    spark: SparkSession,
    pg_url: str,
    jdbc_props: dict,
    sqls: list[str],
) -> None:
    """Execute multiple DML statements over a single JDBC connection.

    Original code called execute_jdbc_update in a loop — one new connection
    per statement.  This version opens once and reuses it for all statements.
    """
    if not sqls:
        return
    conn = None
    stmt = None
    try:
        conn = _get_jdbc_connection(spark, pg_url, jdbc_props)
        stmt = conn.createStatement()
        for sql in sqls:
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
    sqls = []
    for chunk in iter_chunks(ids, 500):
        id_clause = ",".join(str(i) for i in chunk)
        sqls.append(f"""
        UPDATE public.ingest_manifest
        SET processed_flag = {status},
            processed_time = CURRENT_TIMESTAMP,
            error_message = {err_sql}
        WHERE id IN ({id_clause})
          AND batch_id = '{escape_sql_literal(batch_id)}'
        """)
    execute_jdbc_updates_batch(spark, pg_url, jdbc_props, sqls)


def claim_manifest_batch(
    spark: SparkSession,
    pg_url: str,
    jdbc_props: dict,
    job_name: str,
    batch_id: str,
    batch_size: int,
) -> DataFrame:
    """Claim pending manifest rows in 2 JDBC trips instead of 3.

    Original: SELECT pending_ids → UPDATE claim → SELECT claimed (3 trips).
    Optimized: UPDATE with subquery → SELECT claimed (2 trips, no intermediate list).
    """
    claim_sql = f"""
    UPDATE public.ingest_manifest
    SET processed_flag = 9,
        batch_id       = '{escape_sql_literal(batch_id)}',
        error_message  = NULL
    WHERE id IN (
        SELECT id
        FROM   public.ingest_manifest
        WHERE  job_name       = '{escape_sql_literal(job_name)}'
          AND  processed_flag = 0
        ORDER BY ingest_time ASC, id ASC
        LIMIT {batch_size}
    )
    """
    execute_jdbc_update(spark, pg_url, jdbc_props, claim_sql)

    claimed_query = f"""(
        SELECT id, s3_path
        FROM   public.ingest_manifest
        WHERE  job_name       = '{escape_sql_literal(job_name)}'
          AND  processed_flag = 9
          AND  batch_id       = '{escape_sql_literal(batch_id)}'
        ORDER BY ingest_time ASC, id ASC
    ) claimed"""
    return read_jdbc_table(spark, claimed_query, pg_url, jdbc_props)


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


def read_manifest_csv_with_schema(
    spark: SparkSession,
    paths: list[str],
    schema: StructType,
) -> DataFrame:
    if not paths:
        return spark.createDataFrame([], "_source_file string, _ingested_at timestamp")
    schema_map = {f.name: f.dataType for f in schema.fields}
    expected_cols = list(schema_map.keys())
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(paths)
    )
    projected_cols = [
        col(c).cast(schema_map[c]).alias(c) if c in df_raw.columns
        else lit(None).cast(schema_map[c]).alias(c)
        for c in expected_cols
    ]
    missing_cols = [c for c in expected_cols if c not in df_raw.columns]
    if missing_cols:
        logger.warning("CSV header missing expected columns=%s", missing_cols)
    return (
        df_raw.select(*projected_cols)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )


def build_empty_raw_df(spark: SparkSession, important_cols: list[str]) -> DataFrame:
    fields = [StructField(c, StringType(), True) for c in important_cols]
    fields.extend([
        StructField("_source_file", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
    ])
    return spark.createDataFrame([], StructType(fields))


def process_flexi(df_flexi: DataFrame) -> DataFrame:
    flexi_usage_date = spark_coalesce(
        to_date(col("record_opening_time"), "yyyy-MM-dd"),
        to_date(col("record_opening_time"), "yyyyMMdd"),
        to_date(to_timestamp(col("record_opening_time"), "yyyy-MM-dd HH:mm:ss")),
        to_date(to_timestamp(col("record_opening_time"), "yyyyMMddHHmmss")),
        to_date(to_timestamp(col("record_opening_time"), "dd/MM/yyyy HH:mm:ss")),
        to_date(to_timestamp(regexp_replace(trim(col("record_opening_time")), r"^[A-Za-z]{3}\s+", ""), "MMM dd HH:mm:ss yyyy")),
    )
    return (
        df_flexi
        .withColumn("usage_date_tmp", flexi_usage_date)
        .withColumn("year", year(col("usage_date_tmp")).cast("int"))
        .withColumn("month", month(col("usage_date_tmp")).cast("int"))
        .withColumn("day", dayofmonth(col("usage_date_tmp")).cast("int"))
        .drop("usage_date_tmp")
    )


def process_icc(df_icc: DataFrame) -> DataFrame:
    icc_usage_date = spark_coalesce(
        to_date(to_timestamp(col("call_sta_time"), "dd/MM/yyyy HH:mm:ss")),
        to_date(col("call_sta_time"), "yyyy-MM-dd"),
        to_date(col("call_sta_time"), "yyyyMMdd"),
    )
    return (
        df_icc
        .withColumn("used_duration", col("used_duration").cast("double"))
        .withColumn("usage_date_tmp", icc_usage_date)
        .withColumn("year", year(col("usage_date_tmp")).cast("int"))
        .withColumn("month", month(col("usage_date_tmp")).cast("int"))
        .withColumn("day", dayofmonth(col("usage_date_tmp")).cast("int"))
        .drop("usage_date_tmp")
    )
