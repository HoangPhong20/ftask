🚀 End-to-End Data Pipeline (NiFi → Spark → MinIO → PostgreSQL → Metabase)
📌 Overview
CSV (./input)
   ↓
NiFi (ingest)
   ↓
MinIO (raw)
   ↓
Spark (transform)
   ↓
MinIO (processed + curated)
   ↓
PostgreSQL (warehouse)
   ↓
Metabase (dashboard)

🧠 Tech Stack & Rationale
Tool	Purpose
NiFi	Data ingestion, routing, monitoring
Spark	Distributed data processing & ETL
MinIO	S3-compatible data lake storage
PostgreSQL	Data warehouse (fact/dimension)
Metabase	Visualization & dashboard

🐳 1. Start Services
docker compose down
docker compose up -d --build

⏱️ Wait ~1 minutes for all services to be ready.

▶️ Run with Metabase
docker compose --profile bi up -d --build

🔍 2. Service Endpoints
Service	URL
NiFi	http://localhost:8080

MinIO	http://localhost:9001

Spark UI	http://localhost:8081

Metabase	http://localhost:3000

📂 3. Prepare Input Data

Put CSV files into:

./input

Example:

frt_flexi_export_20260321.csv
frt_in_icc_export_20260321.csv

🌊 4. NiFi Flow (CSV → MinIO)
Flow
ListFile_all
  → FetchFile_all
  → RouteOnAttribute_csv
      → PutS3Object_flexi
      → PutS3Object_icc

Key Config

ListFile_all
Input Directory: /data/input
Recurse: true
^frt_(flexi|in_icc)_export_.*\.csv$

FetchFile_all
${absolute.path}${filename}
RouteOnAttribute_csv
${filename:startsWith('frt_flexi_export_')}
${filename:startsWith('frt_in_icc_export_')}

PutS3Object
raw/${filename}
Bucket: datalake
Endpoint: http://minio:9000
Path Style: true

▶️ Start Order
PutS3Object
RouteOnAttribute
FetchFile
ListFile

🧪 5. Validate RAW Data

Open:

👉 http://localhost:9001

Check:

datalake/raw/frt_flexi_export_*.csv
datalake/raw/frt_in_icc_export_*.csv

⚡ 6. Run Spark Job
docker exec -it spark-master spark-submit /opt/spark/jobs/transform.py

📊 7. Check Output
datalake/processed/frt_flexi_raw/
datalake/processed/frt_icc_raw/
datalake/curated/fact_usage_daily/

👉 Presence of .parquet files = success

🗄️ 8. PostgreSQL Tables
public.stg_frt_flexi_raw
public.stg_frt_icc_raw
public.fact_usage_daily

📈 9. Metabase Dashboard

👉 http://localhost:3000

Connection settings:

Host: postgres
Port: 5432
DB: postgres
User: postgres

🔁 10. Re-run Pipeline

If NiFi does not reprocess files:

Stop ListFile_all
Clear state
Empty queue
Start again
🎯 Result
✅ Data ingestion (NiFi)
✅ Data lake storage (MinIO)
✅ Data processing (Spark)
✅ Data warehouse (PostgreSQL)
✅ BI dashboard (Metabase)