# NiFi Data Platform (Real CSV -> NiFi -> MinIO -> Spark -> PostgreSQL)

Pipeline:

`Real CSV -> NiFi (ingest) -> MinIO raw -> Spark -> PostgreSQL (staging + dwh)`

## Current Architecture

- NiFi ingest real files:
  - `frt_flexi_export_*.csv`
  - `frt_in_icc_export_*.csv`
- MinIO raw:
  - `datalake/raw/flexi/*.csv`
  - `datalake/raw/icc/*.csv`
- Spark jobs:
  - `spark/jobs/inspect_raw.py` (check header + 10 rows)
  - `spark/jobs/transform.py` (clean null/duplicate, write MinIO + PostgreSQL)
- PostgreSQL targets:
  - `public.stg_frt_flexi_raw`
  - `public.stg_frt_in_icc_raw`
  - `dwh.fact_usage_daily`

## Important Paths

- Host input root: `./input`
- Local jars bundle: `./jars_spark`
- NiFi input mount: `/data/input`
- Real sample folders:
  - `./input/frt_flexi_export_20260321/`
  - `./input/frt_in_icc_export_20260321/`

`input/` and `jars_spark/` are local-only directories and are ignored by Git.

## Quick Start

1. Build and start:

```powershell
docker compose up -d --build
```

2. Open:
- NiFi: `http://localhost:8080/nifi`
- MinIO: `http://localhost:9001`
- Spark UI: `http://localhost:8081`

3. Setup NiFi flow from:
- `nifi/FLOW_SETUP.md`

4. Validate raw files on MinIO:
- `datalake/raw/flexi/`
- `datalake/raw/icc/`

5. Inspect raw schema quickly:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/inspect_raw.py
```

6. Run transform:

```powershell
python .\scripts\run_spark_job.py
```

## Query PostgreSQL

```sql
SELECT COUNT(*) FROM public.stg_frt_flexi_raw;
SELECT COUNT(*) FROM public.stg_frt_in_icc_raw;
SELECT * FROM dwh.fact_usage_daily ORDER BY usage_date DESC LIMIT 20;
```

## Notes

- NiFi flow uses `ListFile -> FetchFile -> RouteOnAttribute -> PutS3Object`.
- `ListFile` has state; clear state to re-process same files.
- `FetchFile` should use:
  - `${absolute.path}${filename}`
- Spark `transform.py` already includes:
  - null normalization
  - duplicate removal
  - logging before/after row counts.
