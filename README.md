# End-to-End Data Pipeline (NiFi -> Spark -> MinIO -> PostgreSQL -> FastAPI)

## Overview

```text
CSV (./input)
   |
   v
NiFi (ingest)
   |
   v
MinIO (raw)
   |
   v
Spark (transform)
   |
   v
MinIO (processed + curated)
   |
   v
PostgreSQL (warehouse)
   |
   v
FastAPI (web/app endpoints)
```

## Tech Stack and Rationale

| Tool | Purpose |
|---|---|
| NiFi | Data ingestion, routing, monitoring |
| Spark | Distributed data processing and ETL |
| MinIO | S3-compatible data lake storage |
| PostgreSQL | Data warehouse (fact/dimension) |
| FastAPI | API layer for web/app clients |

## 1. Start Services

```bash
docker compose down
docker compose up -d --build
```

Wait around 1 minute for all services to be ready.

## 2. Service Endpoints

| Service | URL |
|---|---|
| NiFi | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Spark UI | http://localhost:8081 |
| FastAPI | http://localhost:8000 |
| FastAPI Docs | http://localhost:8000/docs |

## 3. Prepare Input Data

Put CSV files into:

```text
./input
```

Example:

```text
frt_flexi_export_20260321.csv
frt_in_icc_export_20260321.csv
```

## 4. NiFi Flow (CSV -> MinIO)

Flow:

```text
ListFile_all
  -> FetchFile_all
  -> RouteOnAttribute_csv
      -> PutS3Object_flexi
      -> PutS3Object_icc
```

Key config:

### ListFile_all

- Input Directory: `/data/input`
- Recurse: `true`
- File Filter: `^frt_(flexi|in_icc)_export_.*\.csv$`

### FetchFile_all

- Path: `${absolute.path}${filename}`

### RouteOnAttribute_csv

- flexi: `${filename:startsWith('frt_flexi_export_')}`
- icc: `${filename:startsWith('frt_in_icc_export_')}`

### PutS3Object

- Object Key: `raw/${filename}`
- Bucket: `datalake`
- Endpoint: `http://minio:9000`
- Path Style Access: `true`

Start order:

```text
PutS3Object
RouteOnAttribute
FetchFile
ListFile
```

## 5. Validate RAW Data

Open:

`http://localhost:9001`

Check:

```text
datalake/raw/frt_flexi_export_*.csv
datalake/raw/frt_in_icc_export_*.csv
```

## 6. Run Spark Job

```bash
docker exec -it spark-master spark-submit /opt/spark/jobs/transform.py
```

## 7. Check Output

```text
datalake/processed/frt_flexi_raw/
datalake/processed/frt_icc_raw/
datalake/curated/fact_usage_daily/
```

Presence of `.parquet` files means success.

## 8. PostgreSQL Tables

```text
public.stg_frt_flexi_raw
public.stg_frt_in_icc_raw
dwh.fact_usage_daily
```

## 9. API Endpoints for Client (Web/App)

Core endpoints:

- `GET /health`
- `GET /api/v1/staging/flexi?limit=100`
- `GET /api/v1/staging/icc?limit=100`
- `GET /api/v1/usage/daily?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD&call_type_code=VOICE`

Optional (if you still need BI later):

```bash
docker compose --profile bi up -d --build
```

## 10. Re-run Pipeline

If NiFi does not reprocess files:

1. Stop `ListFile_all`
2. Clear state
3. Empty queue
4. Start again

## Result

- Data ingestion (NiFi)
- Data lake storage (MinIO)
- Data processing (Spark)
- Data warehouse (PostgreSQL)
- API serving layer (FastAPI)
