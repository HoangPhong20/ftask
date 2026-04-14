-- Create Metabase application database during first Postgres initialization.
-- This script only runs when the Postgres data volume is empty.
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'metabase'
)\gexec
