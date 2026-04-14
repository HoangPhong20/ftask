-- 01_init_employees.sql
-- Reworked for real CSV pipeline (Flexi + ICC), no demo employees/attendance tables.

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;

-- Optional: keep extension bootstrap here for future analytics needs.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

