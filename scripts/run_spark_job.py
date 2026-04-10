import os
import subprocess
import sys
import logging
from typing import List
from pathlib import Path

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("run_spark_job")


def load_dotenv_file() -> dict[str, str]:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        return values

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    return values


DOTENV_VALUES = load_dotenv_file()


def get_env(name: str, default: str) -> str:
    # Priority: explicit environment variable > .env file > fallback default.
    return os.getenv(name) or DOTENV_VALUES.get(name, default)


def build_spark_submit_command() -> List[str]:
    # Core config
    job_path = get_env("SPARK_JOB_PATH", "/opt/spark/jobs/transform.py")
    master = get_env("SPARK_MASTER_URL", "spark://spark-master:7077")

    # Resources
    total_executor_cores = get_env("SPARK_TOTAL_EXECUTOR_CORES", "3")
    executor_cores = get_env("SPARK_EXECUTOR_CORES", "1")
    executor_memory = get_env("SPARK_EXECUTOR_MEMORY", "1g")

    # JAR paths
    jars = [
        get_env("SPARK_HADOOP_AWS_JAR", "/opt/spark/jars/hadoop-aws-3.3.4.jar"),
        get_env("SPARK_AWS_SDK_BUNDLE_JAR", "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"),
        get_env("SPARK_POSTGRES_JDBC_JAR", "/opt/spark/jars/postgresql-42.7.4.jar"),
    ]

    jars_str = ",".join(jars)

    # Hadoop S3 config (MinIO)
    hadoop_conf = [
        "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        f"spark.hadoop.fs.s3a.endpoint={get_env('MINIO_ENDPOINT', 'http://minio:9000')}",
        f"spark.hadoop.fs.s3a.access.key={get_env('MINIO_ACCESS_KEY', 'minioadmin')}",
        f"spark.hadoop.fs.s3a.secret.key={get_env('MINIO_SECRET_KEY', '12345678')}",
        "spark.hadoop.fs.s3a.path.style.access=true",
    ]

    # Build command
    cmd = [
        "docker",
        "compose",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", master,
        "--deploy-mode", "client",
        # 🔥 FIX CLASSLOADER (quan trọng nhất)
        "--conf", "spark.driver.extraClassPath=/opt/spark/jars/*",
        "--conf", "spark.executor.extraClassPath=/opt/spark/jars/*",
    ]

    # Add Hadoop configs
    for conf in hadoop_conf:
        cmd.extend(["--conf", conf])

    # Add resources
    cmd.extend([
        "--total-executor-cores", total_executor_cores,
        "--executor-cores", executor_cores,
        "--executor-memory", executor_memory,

        "--jars", jars_str,

        job_path,
    ])

    return cmd


def run():
    cmd = build_spark_submit_command()

    logger.info("Running Spark job command")
    logger.info("%s", " ".join(cmd))

    try:
        result = subprocess.run(cmd, check=False)
    except Exception:
        logger.exception("Failed to execute spark-submit command")
        return 1

    if result.returncode != 0:
        logger.error("Spark job failed with return code %s", result.returncode)
    else:
        logger.info("Spark job completed successfully")

    return result.returncode


if __name__ == "__main__":
    sys.exit(run())
