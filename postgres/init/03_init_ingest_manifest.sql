-- 03_init_ingest_manifest.sql
-- Manifest table used to coordinate NiFi ingestion and Spark incremental processing.

CREATE TABLE IF NOT EXISTS public.ingest_manifest (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    s3_path VARCHAR(1000) NOT NULL,
    ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_flag SMALLINT NOT NULL DEFAULT 0,
    processed_time TIMESTAMP NULL,
    batch_id VARCHAR(64) NULL,
    error_message VARCHAR(2000) NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ingest_manifest_job_path
ON public.ingest_manifest (job_name, s3_path);

CREATE INDEX IF NOT EXISTS idx_ingest_manifest_job_flag_time
ON public.ingest_manifest (job_name, processed_flag, ingest_time);

