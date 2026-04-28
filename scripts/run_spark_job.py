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
    job_path = get_env("SPARK_JOB_PATH", "/opt/project/spark/jobs/transform.py")
    master = get_env("SPARK_MASTER_URL", "spark://spark-master:7077")

    # Resources
    total_executor_cores = get_env("SPARK_TOTAL_EXECUTOR_CORES", "4")
    executor_cores = get_env("SPARK_EXECUTOR_CORES", "2")
    executor_memory = get_env("SPARK_EXECUTOR_MEMORY", "3g")
    driver_memory = get_env("SPARK_DRIVER_MEMORY", "3g")
    executor_memory_overhead = get_env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "512")
    app_name = get_env("SPARK_APP_NAME", "nifi-transform-usage-daily")

    # Build command
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
    ]

    # Forward selected .env keys into the spark-master exec process.
    for key in [
        "PG_JDBC_URL",
        "PG_USER",
        "PG_PASSWORD",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
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
        "INGEST_MANIFEST_JOB_NAME",
        "INGEST_MANIFEST_BATCH_SIZE",
        "CURATED_FACT_OUTPUT_PARTITIONS",
        "PROCESSED_OUTPUT_PARTITIONS",
        "ENABLE_SALT_AGG",
        "SALT_BUCKETS",
        "CALL_TYPE_BROADCAST_THRESHOLD",
        "JDBC_BATCH_SIZE",
        "JDBC_NUM_PARTITIONS",
        "JDBC_WRITE_MAX_RETRIES",
        "JDBC_WRITE_RETRY_SLEEP_SECONDS",
    ]:
        value = get_env(key, "")
        if value:
            cmd.extend(["-e", f"{key}={value}"])

    cmd.extend([
        "spark-master",
        "spark-submit",
        "--master", master,
        "--deploy-mode", "client",
        "--name", app_name,
        "--driver-memory", driver_memory,
        "--conf", f"spark.executor.memoryOverhead={executor_memory_overhead}",
        "--conf", f"spark.sql.shuffle.partitions={get_env('SPARK_SHUFFLE_PARTITIONS', '16')}",
        "--conf", f"spark.default.parallelism={get_env('SPARK_DEFAULT_PARALLELISM', '16')}",
    ])

    # Add resources
    cmd.extend([
        "--total-executor-cores", total_executor_cores,
        "--executor-cores", executor_cores,
        "--executor-memory", executor_memory,
        job_path,
    ])

    return cmd


def redact_command_for_logging(cmd: List[str]) -> List[str]:
    redacted: List[str] = []
    secret_tokens = ("PASSWORD", "SECRET", "TOKEN", "KEY")
    for token in cmd:
        if any(marker in token.upper() for marker in secret_tokens):
            if "=" in token:
                key = token.split("=", 1)[0]
                redacted.append(f"{key}=***")
            else:
                redacted.append("***")
        else:
            redacted.append(token)
    return redacted


def main() -> int:
    with stage_timer("build_spark_submit_command"):
        cmd = build_spark_submit_command()

    logger.info("Running Spark job command")
    logger.info("%s", " ".join(redact_command_for_logging(cmd)))

    max_retries = max(int(get_env("SPARK_SUBMIT_MAX_RETRIES", "3")), 1)
    retry_sleep_seconds = max(int(get_env("SPARK_SUBMIT_RETRY_SLEEP_SECONDS", "10")), 0)
    timeout_seconds = max(int(get_env("SPARK_JOB_TIMEOUT", "10800")), 1)
    result = None

    for attempt in range(1, max_retries + 1):
        try:
            with stage_timer(f"spark_submit_attempt_{attempt}"):
                result = subprocess.run(
                    cmd,
                    check=False,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    text=True,
                    timeout=timeout_seconds,
                )
        except Exception:
            logger.exception("Failed to execute spark-submit command (attempt %s/%s)", attempt, max_retries)
            if attempt < max_retries:
                time.sleep(retry_sleep_seconds)
                continue
            return 1

        if result.returncode == 0:
            break

        logger.warning("Spark failed (attempt %s/%s) with return code %s", attempt, max_retries, result.returncode)
        if attempt < max_retries:
            time.sleep(retry_sleep_seconds)

    if result is None:
        logger.error("Spark job did not start")
        return 1
    if result.returncode != 0:
        logger.error("Spark job failed with return code %s", result.returncode)
        logger.error("Check Spark logs above for root cause")
    else:
        logger.info("Spark job completed successfully")

    return result.returncode


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        log_stage_summary()
    sys.exit(exit_code)
