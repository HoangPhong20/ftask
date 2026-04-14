🚀 How to Run the Project (End-to-End Data Pipeline)
📌 Overview

Pipeline xử lý dữ liệu theo luồng:

CSV (local ./input)
   ↓
NiFi (ingest)
   ↓
MinIO (raw layer)
   ↓
Spark (transform)
   ↓
MinIO (processed + curated)
   ↓
PostgreSQL (data warehouse)
   ↓
Metabase (BI / dashboard)
🧠 Why These Technologies?
🔹 Apache NiFi
Dùng để ingest dữ liệu từ file system
Giao diện kéo-thả, dễ quan sát flow
Hỗ trợ routing, retry, monitoring
👉 Phù hợp cho pipeline ingest real-time / near real-time
🔹 Apache Spark
Xử lý dữ liệu lớn (distributed processing)
Hỗ trợ tốt với Parquet, S3 (MinIO)
Dễ mở rộng khi data tăng
👉 Dùng cho transform + ETL logic
🔹 MinIO (S3-compatible storage)
Đóng vai trò data lake
Lưu trữ theo layer:
raw
processed
curated
Tương thích S3 → Spark đọc trực tiếp
👉 Thay thế AWS S3 trong môi trường local
🔹 PostgreSQL
Làm data warehouse (OLAP nhẹ)
Lưu bảng fact/dimension
Dễ query, join, tích hợp BI
👉 Phù hợp cho demo + production nhỏ
🔹 Metabase
Công cụ BI nhẹ, dễ dùng
Tạo dashboard nhanh
Không cần code
👉 Phục vụ visualize dữ liệu
🐳 1. Start toàn bộ hệ thống

Tại root project:

docker compose down
docker compose up -d --build

👉 Thời gian khởi động: ~1 phút

▶️ Nếu muốn chạy cả Metabase
docker compose --profile bi up -d --build
🔍 2. Kiểm tra services
Service	URL
NiFi	http://localhost:8080

MinIO	http://localhost:9001

Spark UI	http://localhost:8081

Metabase	http://localhost:3000

👉 Nếu vào được là OK

📂 3. Chuẩn bị dữ liệu đầu vào

Copy file CSV vào:

./input
Ví dụ:
frt_flexi_export_20260321.csv
frt_in_icc_export_20260321.csv
🌊 4. Chạy NiFi Flow (CSV → MinIO)
Flow
ListFile_all 
  → FetchFile_all 
  → RouteOnAttribute_csv 
      → PutS3Object_flexi
      → PutS3Object_icc
▶️ Cấu hình nhanh
ListFile_all
Input Directory: /data/input
Recurse Subdirectories: true
File Filter:
^frt_(flexi|in_icc)_export_.*\.csv$
FetchFile_all
File to Fetch:
${absolute.path}${filename}
RouteOnAttribute_csv
is_flexi: ${filename:startsWith('frt_flexi_export_')}
is_icc: ${filename:startsWith('frt_in_icc_export_')}
PutS3Object
Bucket: datalake
Key:
raw/${filename}
Endpoint: http://minio:9000
Path Style: true
▶️ Start order
PutS3Object_flexi & icc
RouteOnAttribute
FetchFile
ListFile

👉 CSV sẽ được upload lên MinIO

🧪 5. Validate dữ liệu RAW

Vào:

👉 http://localhost:9001

Kiểm tra:

datalake/raw/frt_flexi_export_*.csv
datalake/raw/frt_in_icc_export_*.csv
⚡ 6. Chạy Spark Job (Transform)
docker exec -it spark-master spark-submit /opt/spark/jobs/transform.py
🔍 Spark xử lý
Đọc dữ liệu từ raw layer
Transform:
select columns
normalize
deduplicate
Ghi ra:
processed/
curated/
📊 7. Kiểm tra output

Trong MinIO:

datalake/processed/frt_flexi_raw/
datalake/processed/frt_icc_raw/
datalake/curated/fact_usage_daily/

👉 Nếu có file parquet → thành công

🗄️ 8. Kiểm tra PostgreSQL

Spark sẽ ghi vào:

public.stg_frt_flexi_raw
public.stg_frt_icc_raw
public.fact_usage_daily
📈 9. Mở Metabase (BI)

👉 http://localhost:3000

Kết nối DB:
Host: postgres
Port: 5432
Database: postgres
User: postgres

👉 Tạo dashboard từ bảng fact_usage_daily

🔁 10. Chạy lại dữ liệu

Nếu cần re-run:

Stop ListFile_all
Clear state
Empty queue
Start lại
🎯 Kết luận

Pipeline hoàn chỉnh:

Ingest (NiFi) ✔
Storage (MinIO) ✔
Processing (Spark) ✔
Warehouse (Postgres) ✔
Visualization (Metabase) ✔

👉 Đây là một kiến trúc data pipeline chuẩn mini data platform 🚀