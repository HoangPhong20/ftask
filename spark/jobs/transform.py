from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from spark.core import (  # noqa: E402
    build_spark,
    env,
    get_flexi_schema,
    get_icc_schema,
    read_jdbc_table,
    with_postgres_jdbc_optimizations,
    write_parquet_optimized,
    write_staging_table,
)
from spark.dwh import (  # noqa: E402
    build_dim_call_type,
    build_dim_date,
    build_fact_usage_daily,
    build_flexi_daily,
    build_usage_summary_daily,
    to_curated_fact,
)
from spark.pipeline import (  # noqa: E402
    build_empty_raw_df,
    claim_manifest_batch,
    execute_jdbc_update,
    has_any_row,
    iter_chunks,
    maybe_materialize,
    normalize_and_dedup,
    parse_nullable_double,
    process_flexi,
    process_icc,
    read_dim_call_type_subset,
    read_manifest_csv_with_schema,
    select_important_columns,
    to_s3a_path,
    update_manifest_status,
)
from spark.utils import log_stage_summary, stage_timer  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("transform")


def main() -> None:
    with stage_timer("spark_job.total"):
        logger.info("Starting Spark job for real Parquet sources")
        with stage_timer("spark_session.build"):
            spark = build_spark()

        bucket = env("DATALAKE_BUCKET", "datalake")
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
        call_type_broadcast_threshold = int(env("CALL_TYPE_BROADCAST_THRESHOLD", "2000"))
        if enable_salt_agg:
            logger.warning("ENABLE_SALT_AGG=true. Keep this ON only when key skew is significant (e.g. top key >30%%).")
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        pg_url = with_postgres_jdbc_optimizations(env("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/postgres"))
        jdbc_props = {
            "user": env("PG_USER", "postgres"),
            "password": env("PG_PASSWORD", "123456"),
            "driver": "org.postgresql.Driver",
        }

        profile_materialize = env("PIPELINE_PROFILE_MATERIALIZE", "false").lower() == "true"
        flexi_schema = get_flexi_schema()
        icc_schema = get_icc_schema()

        with stage_timer("manifest.claim"):
            df_manifest = claim_manifest_batch(spark, pg_url, jdbc_props, manifest_job_name, batch_id, manifest_batch_size)

        if df_manifest.rdd.isEmpty():
            logger.info("No pending rows in ingest_manifest for job=%s. Nothing to process.", manifest_job_name)
            with stage_timer("spark_session.stop"):
                spark.stop()
            return

        claimed_ids: list[int] = []
        flexi_paths: list[str] = []
        icc_paths: list[str] = []
        unknown_ids: list[int] = []

        for row in df_manifest.collect():
            row_id = int(row["id"])
            s3_path = str(row["s3_path"])
            claimed_ids.append(row_id)
            normalized_path = to_s3a_path(s3_path).lower()
            if "frt_flexi_export_" in normalized_path:
                flexi_paths.append(to_s3a_path(s3_path))
            elif "frt_in_ccc_" in normalized_path or "frt_in_icc_" in normalized_path:
                icc_paths.append(to_s3a_path(s3_path))
            else:
                unknown_ids.append(row_id)

        flexi_important_cols = ["charging_id", "record_sequence_number", "record_opening_time", "record_type", "duration", "served_msisdn", "ftp_filename"]
        icc_important_cols = ["org_call_id", "call_reference", "call_sta_time", "call_type", "used_duration"]

        try:
            df_flexi_raw = read_manifest_csv_with_schema(spark, flexi_paths, flexi_schema) if flexi_paths else build_empty_raw_df(spark, flexi_important_cols)
            df_icc_raw = read_manifest_csv_with_schema(spark, icc_paths, icc_schema) if icc_paths else build_empty_raw_df(spark, icc_important_cols)

            df_flexi_raw = select_important_columns(df_flexi_raw, "flexi", flexi_important_cols)
            df_icc_raw = select_important_columns(df_icc_raw, "icc", icc_important_cols)

            df_flexi = normalize_and_dedup(df_flexi_raw, "flexi", flexi_important_cols, ["charging_id", "record_sequence_number", "record_opening_time", "served_msisdn", "ftp_filename"], shuffle_partitions)
            df_icc = normalize_and_dedup(df_icc_raw, "icc", icc_important_cols, ["org_call_id", "call_reference"], shuffle_partitions)

            df_flexi = maybe_materialize(df_flexi, "flexi", profile_materialize)
            df_icc = maybe_materialize(df_icc, "icc", profile_materialize)

            if not has_any_row(df_flexi, "flexi") and not has_any_row(df_icc, "icc"):
                raise RuntimeError("No valid rows after CSV parsing/normalization for claimed manifest batch. Please verify CSV header/format and parsing rules.")

            write_parquet_optimized(df_flexi, f"{processed_base}/frt_flexi_raw/", minio_write_mode, processed_output_partitions)
            write_parquet_optimized(df_icc, f"{processed_base}/frt_in_icc_raw/", minio_write_mode, processed_output_partitions)

            df_flexi_pg = process_flexi(df_flexi)
            df_icc_pg = process_icc(df_icc)
            logger.info("ICC staging dtypes=%s", df_icc_pg.dtypes)

            write_staging_table(df_flexi_pg, "public.stg_frt_flexi_raw", pg_url, jdbc_props, stg_write_mode, jdbc_batch_size, jdbc_num_partitions)
            write_staging_table(df_icc_pg, "public.stg_frt_in_icc_raw", pg_url, jdbc_props, stg_write_mode, jdbc_batch_size, jdbc_num_partitions)

            df_flexi_daily = build_flexi_daily(df_flexi_pg, enable_salt_agg, salt_buckets, parse_nullable_double).persist(StorageLevel.MEMORY_AND_DISK)
            df_flexi_daily = maybe_materialize(df_flexi_daily, "flexi_daily", profile_materialize)

            df_dim_date_candidates = build_dim_date(df_flexi_daily)
            df_dim_date_existing = read_jdbc_table(spark, "dwh.dim_date", pg_url, jdbc_props).select("date_key")
            df_dim_date_new = df_dim_date_candidates.join(df_dim_date_existing, "date_key", "left_anti")
            write_staging_table(df_dim_date_new, "dwh.dim_date", pg_url, jdbc_props, "append", jdbc_batch_size, jdbc_num_partitions)

            df_dim_call_type_candidates = build_dim_call_type(df_flexi_daily)
            df_call_type_codes = df_dim_call_type_candidates.select("call_type_code").dropDuplicates(["call_type_code"])
            call_type_codes = [r["call_type_code"] for r in df_call_type_codes.collect() if r["call_type_code"] is not None]
            df_dim_call_type_existing = read_dim_call_type_subset(spark, pg_url, jdbc_props, call_type_codes).select("call_type_code")
            df_dim_call_type_new = df_dim_call_type_candidates.join(df_dim_call_type_existing, "call_type_code", "left_anti")
            write_staging_table(df_dim_call_type_new, "dwh.dim_call_type", pg_url, jdbc_props, "append", jdbc_batch_size, jdbc_num_partitions)

            df_dim_call_type_all = read_dim_call_type_subset(spark, pg_url, jdbc_props, call_type_codes).select("call_type_key", "call_type_code")
            df_fact_usage_daily = build_fact_usage_daily(df_flexi_daily, df_dim_call_type_all, call_type_broadcast_threshold)
            df_fact_usage_daily_curated = to_curated_fact(df_fact_usage_daily)

            write_parquet_optimized(df_fact_usage_daily_curated, f"{curated_base}/fact_usage_daily/", minio_write_mode, curated_output_partitions)
            write_staging_table(
                df_fact_usage_daily_curated.select("date_key", "call_type_key", "usage_date", col("call_type_code").alias("call_type"), "event_count", "total_used_duration"),
                "dwh.fact_usage_daily", pg_url, jdbc_props, dwh_write_mode, jdbc_batch_size, jdbc_num_partitions,
            )

            df_usage_summary_daily_curated = build_usage_summary_daily(df_fact_usage_daily_curated)
            summary_usage_dates = [r["usage_date"] for r in df_usage_summary_daily_curated.select("usage_date").dropDuplicates(["usage_date"]).collect()]
            for chunk in iter_chunks(summary_usage_dates, 500):
                date_clause = ",".join(f"DATE '{d}'" for d in chunk)
                execute_jdbc_update(spark, pg_url, jdbc_props, f"DELETE FROM dwh.usage_summary_daily WHERE usage_date IN ({date_clause})")
            write_staging_table(df_usage_summary_daily_curated, "dwh.usage_summary_daily", pg_url, jdbc_props, "append", jdbc_batch_size, jdbc_num_partitions)

            df_flexi_daily.unpersist()
            success_ids = [i for i in claimed_ids if i not in unknown_ids]
            update_manifest_status(spark, pg_url, jdbc_props, success_ids, batch_id=batch_id, status=1)
        except Exception as exc:
            failed_ids = [i for i in claimed_ids if i not in unknown_ids]
            update_manifest_status(spark, pg_url, jdbc_props, failed_ids, batch_id=batch_id, status=2, error_message=str(exc))
            raise
        finally:
            with stage_timer("spark_session.stop"):
                spark.stop()

        logger.info("Spark job completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Spark job failed")
        raise
    finally:
        log_stage_summary()
