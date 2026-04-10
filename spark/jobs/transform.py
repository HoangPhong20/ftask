import logging
import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count as spark_count,
    current_timestamp,
    input_file_name,
    lower,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    when,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("transform")


def env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        return default
    return value


def build_spark() -> SparkSession:
    minio_endpoint = env("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = env("MINIO_SECRET_KEY", "12345678")
    spark_extra_jars = ",".join(
        [
            env("SPARK_HADOOP_AWS_JAR", "/opt/spark/jars/hadoop-aws-3.3.4.jar"),
            env(
                "SPARK_AWS_SDK_BUNDLE_JAR",
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            ),
            env("SPARK_POSTGRES_JDBC_JAR", "/opt/spark/jars/postgresql-42.7.4.jar"),
        ]
    )

    return (
        SparkSession.builder.appName("nifi-minio-spark-realdata")
        .config("spark.jars", spark_extra_jars)
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", env("SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.maximum", "50")
        .config("spark.hadoop.fs.s3a.threads.max", "20")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
        .config("spark.hadoop.fs.s3a.multipart.size", "64M")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "64M")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )


def normalize_and_dedup(df: DataFrame, label: str, key_candidates: list[str]) -> DataFrame:
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    row_before = df.count()
    logger.info("[%s] rows before clean=%s", label, row_before)

    # 1) Normalize string columns: trim + map empty/null-like tokens to NULL.
    out = df
    for c, t in df.dtypes:
        if t == "string":
            cleaned = trim(col(c))
            out = out.withColumn(
                c,
                when(
                    cleaned.isNull()
                    | (cleaned == "")
                    | (lower(cleaned) == "null")
                    | (lower(cleaned) == "none")
                    | (lower(cleaned) == "n/a")
                    | (lower(cleaned) == "na"),
                    None,
                ).otherwise(cleaned),
            )

    # 2) Drop rows that have no business data at all.
    business_cols = [c for c in out.columns if not c.startswith("_")]
    if business_cols:
        out = out.dropna(how="all", subset=business_cols)

    # 3) Deduplicate by best available business keys.
    keys = [k for k in key_candidates if k in out.columns]
    if not keys and business_cols:
        keys = business_cols
        logger.warning("[%s] no candidate keys found, fallback dedup by all business cols", label)

    out = out.dropDuplicates(keys)
    out = out.persist(StorageLevel.MEMORY_AND_DISK)
    row_after_dedup = out.count()
    logger.info(
        "[%s] rows after clean=%s removed=%s keys=%s",
        label,
        row_after_dedup,
        row_before - row_after_dedup,
        keys,
    )
    df.unpersist()
    return out


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

    (
        df.repartition(num_partitions)
        .write.mode(mode)
        .option("batchsize", batch_size)
        .option("numPartitions", str(num_partitions))
        .jdbc(pg_url, table_name, properties=jdbc_props)
    )


def main() -> None:
    logger.info("Starting Spark job for real CSV sources")
    spark = build_spark()

    bucket = env("DATALAKE_BUCKET", "datalake")
    raw_base = f"s3a://{bucket}/raw"
    processed_base = f"s3a://{bucket}/processed"
    curated_base = f"s3a://{bucket}/curated"

    # Use dedicated raw prefixes for real files.
    flexi_path = env("RAW_FLEXI_PATH", f"{raw_base}/flexi/*.csv")
    icc_path = env("RAW_ICC_PATH", f"{raw_base}/icc/*.csv")

    minio_write_mode = env("MINIO_WRITE_MODE", "overwrite")
    stg_write_mode = env("STG_WRITE_MODE", "overwrite")
    dwh_write_mode = env("DWH_FACT_WRITE_MODE", "append")
    jdbc_batch_size = env("JDBC_BATCH_SIZE", "1000")
    jdbc_num_partitions = int(env("JDBC_NUM_PARTITIONS", "2"))

    pg_url = env("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres")
    pg_user = env("PG_USER", "postgres")
    pg_pass = env("PG_PASSWORD", "123456")
    jdbc_props = {"user": pg_user, "password": pg_pass, "driver": "org.postgresql.Driver"}

    logger.info("Reading flexi CSV from %s", flexi_path)
    df_flexi_raw = read_csv(spark, flexi_path)
    logger.info("Reading icc CSV from %s", icc_path)
    df_icc_raw = read_csv(spark, icc_path)

    df_flexi = normalize_and_dedup(
        df_flexi_raw,
        "flexi",
        [
            "org_call_id",
            "charging_id",
            "record_sequence_number",
            "record_opening_time",
            "served_msisdn",
            "ftp_filename",
        ],
    )
    df_icc = normalize_and_dedup(
        df_icc_raw,
        "icc",
        [
            "org_call_id",
            "call_reference",
            "calling_isdn",
            "call_sta_time",
            "call_type",
            "ftp_filename",
        ],
    )

    flexi_count = df_flexi.count()
    icc_count = df_icc.count()
    logger.info("Input row counts flexi=%s icc=%s", flexi_count, icc_count)

    if flexi_count == 0 and icc_count == 0:
        raise RuntimeError("No input CSV rows found under raw/flexi or raw/icc")

    # Data lake write (bronze/processed style)
    logger.info("Writing processed parquet zones")
    df_flexi.write.mode(minio_write_mode).parquet(f"{processed_base}/frt_flexi_raw/")
    df_icc.write.mode(minio_write_mode).parquet(f"{processed_base}/frt_in_icc_raw/")

    # PostgreSQL staging write (full raw structure)
    logger.info("Writing staging tables in PostgreSQL")
    write_staging_table(
        df_flexi,
        "public.stg_frt_flexi_raw",
        pg_url,
        jdbc_props,
        stg_write_mode,
        jdbc_batch_size,
        jdbc_num_partitions,
    )
    write_staging_table(
        df_icc,
        "public.stg_frt_in_icc_raw",
        pg_url,
        jdbc_props,
        stg_write_mode,
        jdbc_batch_size,
        jdbc_num_partitions,
    )

    # Simple curated mart from ICC to query quickly in BI/DataGrip.
    logger.info("Building curated daily usage mart from ICC")
    df_icc_daily = (
        df_icc.withColumn(
            "call_start_ts",
            to_timestamp(col("call_sta_time"), "dd/MM/yyyy HH:mm:ss"),
        )
        .withColumn("usage_date", to_date(col("call_start_ts")))
        .withColumn("used_duration_num", col("used_duration").cast("double"))
        .groupBy("usage_date", "call_type")
        .agg(
            spark_count("*").alias("event_count"),
            spark_sum("used_duration_num").alias("total_used_duration"),
        )
    )
    df_icc_daily = df_icc_daily.persist(StorageLevel.MEMORY_AND_DISK)
    df_icc_daily_count = df_icc_daily.count()
    logger.info("Curated daily usage rows=%s", df_icc_daily_count)

    df_icc_daily.write.mode(minio_write_mode).parquet(f"{curated_base}/fact_usage_daily/")

    write_staging_table(
        df_icc_daily,
        "dwh.fact_usage_daily",
        pg_url,
        jdbc_props,
        dwh_write_mode,
        jdbc_batch_size,
        jdbc_num_partitions,
    )

    df_flexi.unpersist()
    df_icc.unpersist()
    df_icc_daily.unpersist()
    spark.stop()
    logger.info("Spark job completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Spark job failed")
        raise
