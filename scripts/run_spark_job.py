import os
import subprocess
import sys
import logging
import time
from collections import defaultdict
from typing import List
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("run_spark_job")
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
    total_executor_cores = get_env("SPARK_TOTAL_EXECUTOR_CORES", "4")
    executor_cores = get_env("SPARK_EXECUTOR_CORES", "2")
    executor_memory = get_env("SPARK_EXECUTOR_MEMORY", "3g")
    driver_memory = get_env("SPARK_DRIVER_MEMORY", "3g")

    # JAR paths
    jars = [
        get_env("SPARK_HADOOP_AWS_JAR", "/opt/spark/jars-ext/hadoop-aws-3.3.4.jar"),
        get_env("SPARK_AWS_SDK_BUNDLE_JAR", "/opt/spark/jars-ext/aws-java-sdk-bundle-1.12.262.jar"),
        get_env("SPARK_POSTGRES_JDBC_JAR", "/opt/spark/jars-ext/postgresql-42.7.4.jar"),
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
    runtime_conf = [
        f"spark.sql.shuffle.partitions={get_env('SPARK_SHUFFLE_PARTITIONS', '16')}",
        f"spark.default.parallelism={get_env('SPARK_DEFAULT_PARALLELISM', '16')}",
        f"spark.sql.files.maxPartitionBytes={get_env('SPARK_FILES_MAX_PARTITION_BYTES', '134217728')}",
        f"spark.sql.files.openCostInBytes={get_env('SPARK_FILES_OPEN_COST_BYTES', '33554432')}",
        f"spark.sql.adaptive.advisoryPartitionSizeInBytes={get_env('SPARK_ADVISORY_PARTITION_SIZE_BYTES', '134217728')}",
        f"spark.sql.autoBroadcastJoinThreshold={get_env('SPARK_AUTO_BROADCAST_JOIN_THRESHOLD', '104857600')}",
        f"spark.driver.maxResultSize={get_env('SPARK_DRIVER_MAX_RESULT_SIZE', '1g')}",
        f"spark.executor.memoryOverhead={get_env('SPARK_EXECUTOR_MEMORY_OVERHEAD', '512')}",
    ]

    # Build command
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
    ]

    # Forward selected .env keys into the spark-master exec process.
    for key in [
        "RAW_FLEXI_PATH",
        "RAW_ICC_PATH",
        "DATALAKE_BUCKET",
        "MINIO_WRITE_MODE",
        "STG_WRITE_MODE",
        "DWH_FACT_WRITE_MODE",
        "SPARK_SHUFFLE_PARTITIONS",
        "SPARK_DEFAULT_PARALLELISM",
        "SPARK_FILES_MAX_PARTITION_BYTES",
        "SPARK_FILES_OPEN_COST_BYTES",
        "SPARK_ADVISORY_PARTITION_SIZE_BYTES",
        "SPARK_AUTO_BROADCAST_JOIN_THRESHOLD",
        "SPARK_DRIVER_MAX_RESULT_SIZE",
        "INGEST_MANIFEST_BATCH_SIZE",
        "CURATED_FACT_OUTPUT_PARTITIONS",
        "ENABLE_SALT_AGG",
        "SALT_BUCKETS",
        "CALL_TYPE_BROADCAST_THRESHOLD",
        "JDBC_BATCH_SIZE",
        "JDBC_NUM_PARTITIONS",
    ]:
        value = get_env(key, "")
        if value:
            cmd.extend(["-e", f"{key}={value}"])

    cmd.extend([
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", master,
        "--deploy-mode", "client",
        "--driver-memory", driver_memory,
        # Keep classpath explicit for driver/executor.
        "--conf", "spark.driver.extraClassPath=/opt/spark/jars/*",
        "--conf", "spark.executor.extraClassPath=/opt/spark/jars/*",
    ])

    # Add Hadoop configs
    for conf in hadoop_conf:
        cmd.extend(["--conf", conf])
    for conf in runtime_conf:
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


def main() -> int:
    with stage_timer("build_spark_submit_command"):
        cmd = build_spark_submit_command()

    logger.info("Running Spark job command")
    logger.info("%s", " ".join(cmd))

    try:
        with stage_timer("spark_submit"):
            result = subprocess.run(
                cmd,
                check=False,
                stdout=sys.stdout,
                stderr=sys.stderr,
                text=True,
                timeout=7200,
            )
    except Exception:
        logger.exception("Failed to execute spark-submit command")
        return 1

    if result.returncode != 0:
        logger.error("Spark job failed with return code %s", result.returncode)
    else:
        logger.info("Spark job completed successfully")

    return result.returncode


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        log_stage_summary()
    sys.exit(exit_code)
