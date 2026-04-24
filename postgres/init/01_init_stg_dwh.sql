-- 01_init_stg_dwh.sql
-- Bootstrap schemas/extensions for real CSV pipeline (Flexi + ICC).

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;

-- Optional: keep extension bootstrap here for future analytics needs.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
