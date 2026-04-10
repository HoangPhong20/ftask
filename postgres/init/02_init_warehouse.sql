-- 02_init_warehouse.sql
-- Warehouse objects for real-data flow.
-- Spark writes full raw CSV to:
--   public.stg_frt_flexi_raw
--   public.stg_frt_in_icc_raw
-- and writes curated aggregate to:
--   dwh.fact_usage_daily

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.fact_usage_daily (
    usage_date DATE,
    call_type VARCHAR(50),
    event_count BIGINT NOT NULL,
    total_used_duration NUMERIC(18,2),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_usage_daily_date_type
ON dwh.fact_usage_daily(usage_date, call_type);

