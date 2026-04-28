from __future__ import annotations

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logger = logging.getLogger("transform")


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


def get_flexi_schema() -> StructType:
    return StructType(
        [
            StructField("charging_id", StringType(), True),
            StructField("record_sequence_number", StringType(), True),
            StructField("record_opening_time", StringType(), True),
            StructField("record_type", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("served_msisdn", StringType(), True),
            StructField("ftp_filename", StringType(), True),
        ]
    )


def get_icc_schema() -> StructType:
    return StructType(
        [
            StructField("org_call_id", StringType(), True),
            StructField("call_reference", StringType(), True),
            StructField("call_sta_time", StringType(), True),
            StructField("call_type", StringType(), True),
            StructField("used_duration", DoubleType(), True),
        ]
    )


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
    return df.repartition(safe_target)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).withColumn("_source_file", input_file_name()).withColumn("_ingested_at", current_timestamp())


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
    max_retries = max(int(env("JDBC_WRITE_MAX_RETRIES", "3")), 1)
    retry_sleep_seconds = max(float(env("JDBC_WRITE_RETRY_SLEEP_SECONDS", "5")), 0.0)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            (
                with_target_partitions(df, target_partitions)
                .write.mode(mode)
                .option("batchsize", batch_size)
                .option("numPartitions", str(target_partitions))
                .option("isolationLevel", "NONE")
                .option("truncate", "false")
                .jdbc(pg_url, table_name, properties=jdbc_props)
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            logger.warning(
                "JDBC write failed table=%s attempt=%s/%s; retrying in %.1fs; error=%s",
                table_name,
                attempt,
                max_retries,
                retry_sleep_seconds,
                exc,
            )
            time.sleep(retry_sleep_seconds)

    assert last_error is not None
    raise last_error


def write_parquet_optimized(df: DataFrame, path: str, mode: str, num_partitions: int) -> None:
    target_partitions = max(num_partitions, 1)
    logger.info(
        "Writing parquet path=%s mode=%s partitions=%s",
        path,
        mode,
        target_partitions,
    )
    df_to_write = df.coalesce(target_partitions) if df.rdd.getNumPartitions() > target_partitions else df
    df_to_write.write.mode(mode).option("compression", "snappy").parquet(path)


def read_jdbc_table(spark: SparkSession, table_name: str, pg_url: str, jdbc_props: dict) -> DataFrame:
    # table_name accepts either a table name or a subquery string "(SELECT ...) alias".
    return (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", table_name)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .option("driver", jdbc_props["driver"])
        .option("fetchsize", "10000")
        .load()
    )
