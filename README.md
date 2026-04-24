# Nifi Realtime Pipeline

End-to-end analytics pipeline for ingesting CSV files with NiFi, transforming them with Spark, loading data into PostgreSQL, and serving dashboard APIs with FastAPI.

## Overview

```text
CSV -> NiFi -> MinIO -> Spark ETL -> PostgreSQL -> FastAPI -> Dashboard / Metabase
```

The project is built to keep dashboard reads fast by using:

- raw and staging layers
- a DWH fact table for detail queries
- a summary mart for dashboard cards and trend charts
- short TTL cache in the API layer

## NiFi Config

NiFi is configured in `docker-compose.yml`.

Key settings:

- Web UI: `http://localhost:8080`
- Flow config mount: `./nifi/conf -> /opt/nifi/nifi-current/conf`
- FlowFile repository: `./nifi/flowfile`
- Content repository: `./nifi/database`
- State directory: `./nifi/state`
- Input mount: `/mnt/d/data/Nifi-input -> /data/input`
- Driver/JAR mount: `./jars_spark -> /opt/nifi/drivers:ro`

Notes:

- NiFi flow state is persisted in the mounted `nifi/` folders.
- Source CSV files should be placed in `/mnt/d/data/Nifi-input`.
- If you edit the flow in NiFi UI, the config is kept in `nifi/conf/flow.xml.gz`.

## Main Tables

```text
public.ingest_manifest
public.stg_frt_flexi_raw
public.stg_frt_in_icc_raw
dwh.dim_date
dwh.dim_call_type
dwh.fact_usage_daily
dwh.usage_summary_daily
```

## API

### Public

- `GET /health`
- `GET /api/usage/daily`
- `GET /api/analytics/metrics/usage-summary`
- `GET /api/analytics/metrics/usage-trend`

### Internal

- `GET /internal/staging/flexi`
- `GET /internal/staging/icc`

## Data Usage

- `dwh.fact_usage_daily`: detail / drill-down
- `dwh.usage_summary_daily`: dashboard summary / trend

## Performance

For summary range queries, keep this index:

```sql
CREATE INDEX IF NOT EXISTS idx_usage_summary_usage_date_call_type
ON dwh.usage_summary_daily (usage_date, call_type_code);
```

The API also uses a short TTL in-memory cache for hot dashboard requests.

## Project Layout

```text
api/                 FastAPI app
spark/jobs/          Spark ETL job
scripts/             Spark helper script
postgres/init/       PostgreSQL init SQL
docker-compose.yml   Local stack
```

## Run

Use a terminal with Docker/Docker Compose installed. The services themselves run inside containers.

Start all services:

```bash
docker compose up -d --build
```

Rebuild API only:

```bash
docker compose up -d --build api
```

Run the Spark job directly inside the Spark container:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/transform.py
```

## URLs

| Service | URL |
|---|---|
| NiFi | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Spark UI | http://localhost:8081 |
| FastAPI | http://localhost:8000 |
| FastAPI Docs | http://localhost:8000/docs |
| Metabase | http://localhost:3000 |

## Useful Commands

Reset ingest flags:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "UPDATE public.ingest_manifest SET processed_flag=0, processed_time=NULL WHERE job_name='transform_usage_daily';"
```

Truncate staging and analytical tables:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "TRUNCATE TABLE public.stg_frt_flexi_raw, public.stg_frt_in_icc_raw, dwh.fact_usage_daily, dwh.usage_summary_daily;"
```
