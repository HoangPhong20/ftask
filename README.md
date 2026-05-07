# Telecom Usage ETL Pipeline

End-to-end batch data engineering project for processing telecom usage CSV files into analytics-ready datasets and API/dashboard outputs.

The pipeline ingests raw telecom records with Apache NiFi, stores files in MinIO as a local data lake, transforms and aggregates data with Apache Spark, loads curated tables into PostgreSQL, then serves analytics through FastAPI and Metabase.

```text
CSV files -> Apache NiFi -> MinIO -> Apache Spark ETL -> PostgreSQL DWH -> FastAPI / Metabase
```

## Overview

This project solves the problem of turning large raw telecom usage files into clean, queryable business data.

It is designed for batch processing of telecom CSV datasets such as Flexi and ICC usage records. Raw files are tracked through an ingestion manifest, processed incrementally by Spark, written back to MinIO as Parquet, and loaded into PostgreSQL staging and warehouse tables.

The final output is a daily usage analytics model that can be queried directly from PostgreSQL, exposed through REST APIs, or visualized in Metabase.

## Architecture

```text
                         +----------------+
                         |  Telecom CSVs  |
                         +-------+--------+
                                 |
                                 v
                         +-------+--------+
                         | Apache NiFi    |
                         | File ingestion |
                         +-------+--------+
                                 |
                    raw CSV      | manifest rows
                                 v
+----------------+       +-------+--------+       +----------------------+
| MinIO Data Lake|<------| PostgreSQL     |------>| Spark ETL Job        |
| raw/processed  |       | ingest_manifest|       | clean/transform/load |
+-------+--------+       +----------------+       +----------+-----------+
        ^                                                   |
        | processed Parquet                                 v
        |                                          +--------+---------+
        +------------------------------------------| PostgreSQL DWH   |
                                                   | staging/star     |
                                                   +--------+---------+
                                                            |
                                      +---------------------+---------------------+
                                      v                                           v
                              +-------+--------+                          +-------+--------+
                              | FastAPI        |                          | Metabase       |
                              | REST endpoints |                          | BI dashboard   |
                              +----------------+                          +----------------+
```

### Data Flow

1. NiFi detects new CSV files and uploads them to MinIO.
2. NiFi inserts each file path into `public.ingest_manifest`.
3. Spark claims pending manifest rows for the current batch.
4. Spark reads raw CSV files from MinIO using S3A.
5. Spark cleans, normalizes, deduplicates, and transforms telecom records.
6. Spark writes processed Parquet files back to MinIO.
7. Spark loads cleaned data into PostgreSQL staging tables.
8. Spark builds warehouse dimensions, facts, and summary tables.
9. FastAPI and Metabase read analytics data from PostgreSQL.

## Features

- Batch CSV ingestion using Apache NiFi
- Raw and processed data lake storage with MinIO
- Manifest-based incremental processing
- Spark ETL for cleansing, normalization, deduplication, and aggregation
- Processed Parquet output partitioned by `batch_id`
- PostgreSQL staging tables for validated raw-like records
- Star schema warehouse with date and call type dimensions
- Daily usage fact and summary tables for analytics
- FastAPI endpoints for usage and trend metrics
- Optional Metabase dashboard layer
- Docker Compose deployment for local development
- Configurable Spark resources, JDBC write settings, and batch size through `.env`

## Tech Stack

| Area | Technology |
|---|---|
| Orchestration / ingestion | Apache NiFi |
| Distributed processing | Apache Spark, PySpark |
| Data lake | MinIO, S3A |
| Data warehouse | PostgreSQL |
| API | FastAPI, Python |
| BI | Metabase |
| Runtime | Docker, Docker Compose |
| Storage formats | CSV, Parquet |

## Project Structure

```text
.
├── api/                    # FastAPI application
│   ├── app/api/             # API routers and endpoints
│   ├── app/repositories/    # PostgreSQL query layer
│   ├── app/services/        # Business logic
│   ├── Dockerfile
│   └── requirements.txt
├── jars_spark/              # External Spark/NiFi JAR files
├── nifi/                    # Local NiFi configuration and flow state
├── postgres/init/           # PostgreSQL bootstrap SQL
├── scripts/                 # Helper scripts
│   └── run_spark_job.py     # Recommended Spark job runner
├── spark/                   # Spark ETL source code
│   ├── jobs/transform.py    # Main Spark entrypoint
│   ├── core.py
│   ├── dwh.py
│   ├── pipeline.py
│   └── utils.py
├── docker-compose.yml
├── .env
└── README.md
```

## Data Model

The warehouse uses a small star schema for daily telecom usage analytics.

### Staging Tables

| Table | Description |
|---|---|
| `public.stg_frt_flexi_raw` | Cleaned Flexi source records loaded from raw CSV |
| `public.stg_frt_in_icc_raw` | Cleaned ICC source records loaded from raw CSV |

### Warehouse Tables

| Table | Description |
|---|---|
| `dwh.dim_date` | Date dimension with day, month, quarter, year, week, and weekend attributes |
| `dwh.dim_call_type` | Call type dimension built from normalized telecom record types |
| `dwh.fact_usage_daily` | Daily usage fact table with event count and total used duration |
| `dwh.usage_summary_daily` | Aggregated serving table for APIs and dashboards |

### Fact Grain

`dwh.fact_usage_daily` is aggregated by:

- `usage_date`
- `call_type`

Main metrics:

- `event_count`
- `total_used_duration`

Current note: the curated daily fact and summary are built from Flexi records. ICC records are currently loaded to staging for validation and future extension.

## Prerequisites

- Docker
- Docker Compose
- Python 3.10+ if running helper scripts from the host machine

## Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd Nifi
```

### 2. Configure Environment

Review `.env` before starting the stack. Important settings include:

```text
INGEST_MANIFEST_BATCH_SIZE=5
SPARK_SHUFFLE_PARTITIONS=16
SPARK_DEFAULT_PARALLELISM=16
PROCESSED_OUTPUT_PARTITIONS=8
CURATED_FACT_OUTPUT_PARTITIONS=8
JDBC_NUM_PARTITIONS=4
JDBC_BATCH_SIZE=5000
ENABLE_SALT_AGG=false
```

### 3. Start Services

```bash
docker compose up -d
```

Start with rebuild:

```bash
docker compose up -d --build
```

Start with Metabase:

```bash
docker compose --profile bi up -d
```

Check service status:

```bash
docker compose ps
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
| Metabase | http://localhost:3000 |

`http://localhost:4040` is available only while a Spark job is running.

## Usage

### 1. Ingest CSV Files

Place telecom CSV files in the input location configured in NiFi. NiFi uploads them to MinIO and records file paths in `public.ingest_manifest`.

### 2. Run Spark ETL

Recommended:

```bash
python scripts/run_spark_job.py
```

On Windows with Python launcher:

```powershell
py scripts/run_spark_job.py
```

On Ubuntu / WSL:

```bash
python3 scripts/run_spark_job.py
```

Direct Spark submit:

```bash
docker compose exec -T spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/project/spark/jobs/transform.py
```

### 3. Query PostgreSQL

Check manifest status:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT processed_flag, count(*)
FROM public.ingest_manifest
WHERE job_name='transform_usage_daily'
GROUP BY processed_flag
ORDER BY processed_flag;
"
```

Check output tables:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT count(*) AS stg_flexi FROM public.stg_frt_flexi_raw;
SELECT count(*) AS stg_icc FROM public.stg_frt_in_icc_raw;
SELECT count(*) AS fact_rows FROM dwh.fact_usage_daily;
SELECT count(*) AS summary_rows FROM dwh.usage_summary_daily;
"
```

Sample analytics query:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT usage_date, call_type_code, event_count, total_used_duration
FROM dwh.usage_summary_daily
ORDER BY usage_date DESC, call_type_code
LIMIT 50;
"
```

### 4. Call API

Health check:

```bash
curl "http://localhost:8000/health"
```

Daily usage:

```bash
curl "http://localhost:8000/api/usage/daily?year=2026&limit=50"
```

Usage summary:

```bash
curl "http://localhost:8000/api/analytics/metrics/usage-summary?from=2026-01-01&to=2026-01-31"
```

Usage trend:

```bash
curl "http://localhost:8000/api/analytics/metrics/usage-trend?from=2026-01-01&to=2026-01-31&grain=day"
```

Connect Metabase to PostgreSQL and build charts from `dwh.usage_summary_daily` or `dwh.fact_usage_daily`.

## Manifest Status

Spark processes files listed in `public.ingest_manifest`.

| Flag | Meaning |
|---:|---|
| `0` | Pending |
| `9` | Claimed / running |
| `1` | Success |
| `2` | Failed |

## Processed Data Paths

Spark writes processed Parquet data to MinIO:

```text
s3a://datalake/processed/frt_flexi_raw/batch_id=<batch_id>/
s3a://datalake/processed/frt_in_icc_raw/batch_id=<batch_id>/
```

## Configuration Notes

| Variable | Purpose |
|---|---|
| `INGEST_MANIFEST_BATCH_SIZE` | Maximum number of files Spark claims per run |
| `SPARK_SHUFFLE_PARTITIONS` | Number of Spark shuffle partitions |
| `SPARK_DEFAULT_PARALLELISM` | Default Spark task parallelism |
| `PROCESSED_OUTPUT_PARTITIONS` | Number of partitions for processed Parquet output |
| `CURATED_FACT_OUTPUT_PARTITIONS` | Number of partitions for curated fact output |
| `JDBC_NUM_PARTITIONS` | DataFrame partitions before JDBC writes |
| `JDBC_BATCH_SIZE` | PostgreSQL insert batch size |
| `ENABLE_SALT_AGG` | Enables salted aggregation when aggregation keys are heavily skewed |

## Why This Project Matters

This project demonstrates a practical data engineering workflow:

- ingesting raw files with NiFi
- storing raw and processed data in a lake
- running distributed ETL with Spark
- designing warehouse tables for analytics
- exposing data through APIs and BI tools
- packaging the system with Docker Compose

