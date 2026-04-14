-- 02_init_warehouse.sql
-- Warehouse objects for real-data flow.
-- Spark writes full raw CSV to:
--   public.stg_frt_flexi_raw
--   public.stg_frt_in_icc_raw
-- and writes curated aggregate to:
--   dwh.dim_date
--   dwh.dim_call_type
--   dwh.fact_usage_daily

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month SMALLINT NOT NULL,
    month_of_year SMALLINT NOT NULL,
    quarter_of_year SMALLINT NOT NULL,
    year_number INTEGER NOT NULL,
    week_of_year SMALLINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dwh.dim_call_type (
    call_type_key BIGSERIAL PRIMARY KEY,
    call_type_code VARCHAR(50) NOT NULL UNIQUE,
    call_type_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dwh.fact_usage_daily (
    date_key INTEGER,
    call_type_key BIGINT,
    usage_date DATE,
    call_type VARCHAR(50),
    event_count BIGINT NOT NULL,
    total_used_duration NUMERIC(18,2),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE dwh.fact_usage_daily
    ADD COLUMN IF NOT EXISTS date_key INTEGER,
    ADD COLUMN IF NOT EXISTS call_type_key BIGINT;

UPDATE dwh.fact_usage_daily
SET date_key = CAST(to_char(usage_date, 'YYYYMMDD') AS INTEGER)
WHERE date_key IS NULL AND usage_date IS NOT NULL;

UPDATE dwh.fact_usage_daily f
SET call_type_key = d.call_type_key
FROM dwh.dim_call_type d
WHERE f.call_type_key IS NULL
  AND f.call_type IS NOT NULL
  AND upper(trim(f.call_type)) = d.call_type_code;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_fact_usage_daily_date_key'
          AND conrelid = 'dwh.fact_usage_daily'::regclass
    ) THEN
        ALTER TABLE dwh.fact_usage_daily
            ADD CONSTRAINT fk_fact_usage_daily_date_key
            FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key) NOT VALID;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_fact_usage_daily_call_type_key'
          AND conrelid = 'dwh.fact_usage_daily'::regclass
    ) THEN
        ALTER TABLE dwh.fact_usage_daily
            ADD CONSTRAINT fk_fact_usage_daily_call_type_key
            FOREIGN KEY (call_type_key) REFERENCES dwh.dim_call_type(call_type_key) NOT VALID;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date
ON dwh.dim_date(full_date);

CREATE INDEX IF NOT EXISTS idx_dim_call_type_code
ON dwh.dim_call_type(call_type_code);

CREATE INDEX IF NOT EXISTS idx_fact_usage_daily_date_type
ON dwh.fact_usage_daily(date_key, call_type_key);
