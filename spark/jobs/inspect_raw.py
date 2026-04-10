import os

from pyspark.sql import SparkSession


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def build_spark() -> SparkSession:
    spark_jars = ",".join(
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
        SparkSession.builder.appName("inspect-raw-csv")
        .config("spark.jars", spark_jars)
        .config("spark.hadoop.fs.s3a.endpoint", env("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", env("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", env("MINIO_SECRET_KEY", "12345678"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def inspect_csv(spark: SparkSession, label: str, path: str) -> None:    
    print(f"\n=== {label} ===")
    print(f"Path: {path}")
    df = (
        spark.read.option("header", "true")
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    print(f"Row count: {df.count()}")
    print("Columns:")
    print(df.columns)
    print("Top 10 rows:")
    df.show(10, truncate=False)


def main() -> None:
    bucket = env("DATALAKE_BUCKET", "datalake")
    flexi_path = env("RAW_FLEXI_PATH", f"s3a://{bucket}/raw/flexi/*.csv")
    icc_path = env("RAW_ICC_PATH", f"s3a://{bucket}/raw/icc/*.csv")

    spark = build_spark()
    try:
        inspect_csv(spark, "FLEXI", flexi_path)
        inspect_csv(spark, "ICC", icc_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

