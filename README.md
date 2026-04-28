# Nifi Realtime Pipeline

End-to-end pipeline for CSV ingestion with NiFi, transformation with Spark, warehouse loading in PostgreSQL, and serving metrics through FastAPI.

## Architecture

```text
CSV -> NiFi -> MinIO -> Spark ETL -> PostgreSQL -> FastAPI -> Dashboard/Metabase
```

## Project Structure

```text
api/                 FastAPI app
spark/jobs/          Spark ETL job (mounted into Spark containers)
scripts/             Helper scripts (including Spark runner)
postgres/init/       PostgreSQL bootstrap SQL
docker-compose.yml   Local stack
```

## Prerequisites

- Docker + Docker Compose v2
- A valid `.env` file in repo root
- Input CSV files available for NiFi at `/mnt/d/data/Nifi-input` (host path used by compose)

## Start The Stack

Run from repository root:

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
```

When you update only one service:

```bash
docker compose up -d --build api
docker compose up -d --force-recreate spark-master spark-worker
```

## Run Spark ETL

Recommended (uses env from `.env` and forwards runtime options):

Windows:

```bash
py scripts/run_spark_job.py
```

Linux/macOS:

```bash
python3 scripts/run_spark_job.py
```

Direct run inside container (alternative):

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/transform.py
```

## Service URLs

| Service | URL |
|---|---|
| NiFi | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Spark Master UI | http://localhost:8081 |
| Spark Driver UI | http://localhost:4040 |
| Spark Worker UI | http://localhost:8082 |
| FastAPI | http://localhost:8000 |
| FastAPI Docs | http://localhost:8000/docs |
| Metabase (profile: bi) | http://localhost:3000 |

Notes:
- `http://localhost:4040` is available only while a Spark job is running.
- If 4040 is busy, Spark may switch to `4041`, `4042`, etc.

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

## Re-Run And Recovery Commands

Reset ingest manifest completely (full replay):

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "UPDATE public.ingest_manifest SET processed_flag=0, processed_time=NULL, batch_id=NULL, error_message=NULL WHERE job_name='transform_usage_daily';"
```


Check manifest status:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "SELECT processed_flag, count(*) FROM public.ingest_manifest WHERE job_name='transform_usage_daily' GROUP BY processed_flag ORDER BY processed_flag;"
```

Check staging + DWH row counts:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "SELECT count(*) AS stg_flexi FROM public.stg_frt_flexi_raw; SELECT count(*) AS stg_icc FROM public.stg_frt_in_icc_raw; SELECT count(*) AS fact_rows FROM dwh.fact_usage_daily; SELECT count(*) AS summary_rows FROM dwh.usage_summary_daily;"
```

Truncate staging and marts:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "TRUNCATE TABLE public.stg_frt_flexi_raw, public.stg_frt_in_icc_raw, dwh.fact_usage_daily, dwh.usage_summary_daily;"
```

