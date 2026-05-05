# NiFi Realtime Usage Pipeline

Du an nay la mot data pipeline local dung de nap file CSV telecom, luu vao object storage, xu ly bang Spark, dua vao PostgreSQL data warehouse, sau do phuc vu API va dashboard/BI.

Luồng tổng quát:

```text
CSV files -> NiFi -> MinIO/S3 -> Spark ETL -> PostgreSQL DWH -> FastAPI -> Dashboard/Metabase
```

## 1. Du an nay giai quyet viec gi?

Pipeline duoc thiet ke de xu ly du lieu su dung dich vu theo ngay. Hien tai du an co 2 nhom file dau vao:

- `FRT_FLEXI`: du lieu su dung chinh, co truong thoi gian mo record, loai record/cuoc goi, thoi luong, so thue bao.
- `FRT_IN_ICC` hoac `FRT_IN_CCC`: du lieu cuoc goi/usage ICC, co call id, call reference, thoi gian bat dau cuoc goi, call type, used duration.

Spark doc cac file da duoc NiFi dua len MinIO, lam sach du lieu, chuan hoa ngay thang, luu raw da xu ly vao staging PostgreSQL, va tao bang tong hop ngay theo `usage_date` va `call_type`.

Luu y quan trong: bang fact va bang summary hien tai duoc build tu nguon `FRT_FLEXI`. Du lieu ICC van duoc nap, lam sach, ghi vao staging `public.stg_frt_in_icc_raw`, nhung chua duoc dung de tao `dwh.fact_usage_daily`.

## 2. Kien truc thanh phan

| Thanh phan | Vai tro |
|---|---|
| NiFi | Theo doi folder CSV dau vao, dua file len MinIO, va ghi manifest cho Spark biet file nao can xu ly. |
| MinIO | Object storage local, dong vai tro S3-compatible data lake. |
| Spark | ETL engine: doc CSV tu MinIO, clean/dedup, ghi Parquet, load staging, build dimension/fact/summary. |
| PostgreSQL | Luu ingest manifest, staging raw, va data warehouse. |
| FastAPI | Cung cap API doc staging, usage daily, analytics summary/trend. |
| Metabase | Cong cu BI tuy chon, chay voi profile `bi`. |

## 3. Cau truc thu muc

```text
api/                  FastAPI application
api/app/api/           API routers/endpoints
api/app/repositories/  SQLAlchemy query layer
api/app/services/      Business/date validation layer

spark/                 Spark application package
spark/jobs/transform.py Entry point cua Spark ETL job
spark/core.py          SparkSession, schema, read/write helpers
spark/pipeline.py      Ham clean, manifest, parse ngay, process staging
spark/dwh.py           Logic build dimension/fact/summary
spark/utils.py         Runtime stage logging

postgres/init/         SQL bootstrap cho PostgreSQL
scripts/               Script tien ich, gom runner cho Spark job
jars_spark/            JDBC/S3 jars mount vao Spark va NiFi
nifi/                  NiFi config/state local
docker-compose.yml     Local stack
```

## 4. Du lieu dau vao

### 4.1. Flexi CSV

Spark mong doi cac cot sau trong file co ten chua `frt_flexi_export_`:

| Cot | Y nghia trong pipeline |
|---|---|
| `charging_id` | Ma dinh danh record/charging event. Dung lam mot phan khoa dedup. |
| `record_sequence_number` | So thu tu record. Dung lam mot phan khoa dedup. |
| `record_opening_time` | Thoi diem phat sinh usage. Duoc parse thanh `usage_date`, `year`, `month`, `day`. |
| `record_type` | Loai usage/call type. Duoc trim, uppercase, blank thanh `UNKNOWN`. |
| `duration` | Thoi luong su dung. Duoc parse thanh so double khi tinh tong. |
| `served_msisdn` | So thue bao duoc phuc vu. Luu staging de tra cuu. |
| `ftp_filename` | Ten file nguon logic tu he thong upstream. Luu staging de trace. |

Flexi la nguon chinh de tao:

- `dwh.dim_date`
- `dwh.dim_call_type`
- `dwh.fact_usage_daily`
- `dwh.usage_summary_daily`
- Parquet curated tai `s3a://datalake/curated/fact_usage_daily/`

### 4.2. ICC CSV

Spark mong doi cac cot sau trong file co ten chua `frt_in_icc_` hoac `frt_in_ccc_`:

| Cot | Y nghia trong pipeline |
|---|---|
| `org_call_id` | Ma cuoc goi goc. Dung lam mot phan khoa dedup. |
| `call_reference` | Ma tham chieu cuoc goi. Dung lam mot phan khoa dedup. |
| `call_sta_time` | Thoi gian bat dau cuoc goi. Duoc parse thanh `year`, `month`, `day`. |
| `call_type` | Loai cuoc goi/usage cua ICC. |
| `used_duration` | Thoi luong su dung, cast ve double. |

ICC hien duoc ghi vao:

- Parquet processed tai `s3a://datalake/processed/frt_in_icc_raw/batch_id=<batch_id>/`
- PostgreSQL staging `public.stg_frt_in_icc_raw`

## 5. Cac tang du lieu

### 5.1. Manifest

Bang `public.ingest_manifest` la bang dieu phoi incremental processing.

| Cot | Y nghia |
|---|---|
| `id` | Primary key cua manifest row. |
| `job_name` | Ten job Spark, mac dinh `transform_usage_daily`. |
| `s3_path` | Duong dan file CSV tren MinIO/S3. |
| `ingest_time` | Luc NiFi ghi file vao manifest. |
| `processed_flag` | Trang thai xu ly: `0` pending, `9` claimed/running, `1` success, `2` failed. |
| `processed_time` | Thoi diem Spark cap nhat trang thai. |
| `batch_id` | Batch Spark claim file. |
| `error_message` | Loi neu batch xu ly that bai. |

Spark khong scan toan bo bucket moi lan chay. No chi lay nhung row `processed_flag = 0` trong manifest, claim sang `9`, xu ly, roi cap nhat thanh `1` hoac `2`.

### 5.2. Processed Parquet tren MinIO

Sau khi doc CSV va clean/dedup, Spark ghi lai du lieu dang Parquet:

```text
s3a://datalake/processed/frt_flexi_raw/batch_id=<batch_id>/
s3a://datalake/processed/frt_in_icc_raw/batch_id=<batch_id>/
```

Day la tang "silver boundary": Spark doc lai Parquet sau khi ghi de cat lineage tu CSV raw, giup cac buoc downstream on dinh hon va khong bi recompute CSV parsing qua nhieu action.

### 5.3. Staging PostgreSQL

| Bang | Noi dung |
|---|---|
| `public.stg_frt_flexi_raw` | Flexi sau clean/dedup, co them `_source_file`, `_ingested_at`, `year`, `month`, `day`. |
| `public.stg_frt_in_icc_raw` | ICC sau clean/dedup, co them `_source_file`, `_ingested_at`, `year`, `month`, `day`. |

Staging giu du lieu gan voi raw nhat co the, de kiem tra lai record va debug ingestion.

### 5.4. Data warehouse

| Bang | Noi dung |
|---|---|
| `dwh.dim_date` | Dimension ngay, gom `date_key`, ngay, thang, quy, nam, week, weekend. |
| `dwh.dim_call_type` | Dimension loai usage/call type lay tu `record_type` Flexi. |
| `dwh.fact_usage_daily` | Fact theo ngay va call type: so event va tong duration. |
| `dwh.usage_summary_daily` | Bang summary ngay da precompute de API dashboard doc nhanh. |

## 6. Logic Spark xu ly du lieu

Entry point la `spark/jobs/transform.py`. Cac buoc chinh:

### Buoc 1: Khoi tao SparkSession

`build_spark()` trong `spark/core.py` tao SparkSession va cau hinh:

- Ket noi MinIO bang S3A.
- Ket noi PostgreSQL qua JDBC.
- Bat Adaptive Query Execution.
- Cau hinh shuffle partitions, default parallelism, broadcast threshold.
- Cau hinh S3 multipart upload va retry.
- Cau hinh JDBC write batch/retry.

### Buoc 2: Claim batch tu manifest

Spark goi `claim_manifest_batch()`:

1. Tim cac row trong `public.ingest_manifest` co:
   - `job_name = transform_usage_daily`
   - `processed_flag = 0`
2. Lay toi da `INGEST_MANIFEST_BATCH_SIZE` row.
3. Cap nhat chung thanh `processed_flag = 9` va gan `batch_id`.
4. Doc lai cac row vua claim de lay `id` va `s3_path`.

Lam nhu vay giup job Spark xu ly incremental va tranh xu ly lap file da thanh cong.

### Buoc 3: Phan loai file dau vao

Spark nhin vao `s3_path`:

- Neu path chua `frt_flexi_export_` thi dua vao danh sach Flexi.
- Neu path chua `frt_in_icc_` hoac `frt_in_ccc_` thi dua vao danh sach ICC.
- File khong khop pattern duoc xem la unknown va khong tinh la success.

### Buoc 4: Doc CSV voi schema co dinh

Spark khong infer schema tu CSV. File duoc doc bang schema trong `spark/core.py`:

- Flexi: tat ca cot doc dang string.
- ICC: `used_duration` doc dang double, cac cot con lai doc dang string.

Neu CSV thieu cot, Spark tao cot do voi gia tri `NULL` va ghi warning. Cach nay giup job khong vo ngay khi header co thieu cot, nhung van de lo du lieu can kiem tra.

### Buoc 5: Chon cot quan trong, clean, dedup

Spark chi giu cac cot quan trong va metadata:

- `_source_file`: file MinIO/S3 thuc te.
- `_ingested_at`: thoi diem Spark doc/ghi batch.

Sau do clean string:

- Trim khoang trang dau/cuoi.
- Bien chuoi rong thanh `NULL`.
- Bien cac token `null`, `none`, `n/a`, `na` thanh `NULL`.

Dedup:

- Flexi dedup theo `charging_id`, `record_sequence_number`, `record_opening_time`.
- ICC dedup theo `org_call_id`, `call_reference`.
- Neu thieu khoa ung vien, fallback dedup theo tat ca cot business.

### Buoc 6: Ghi processed Parquet va doc lai

Spark ghi Flexi/ICC da clean vao MinIO processed layer, chia theo `batch_id`.

Sau do Spark doc lai Parquet nay. Day la diem cat giua raw CSV parsing va cac buoc JDBC/DWH. Ve mat thuc thi, no giam rui ro Spark phai doc/parse CSV lai nhieu lan khi co nhieu action downstream.

### Buoc 7: Build staging

Flexi:

- Parse `record_opening_time` theo nhieu format:
  - `yyyy-MM-dd`
  - `yyyyMMdd`
  - `yyyy-MM-dd HH:mm:ss`
  - `yyyyMMddHHmmss`
  - `dd/MM/yyyy HH:mm:ss`
  - dang co prefix thu trong tieng Anh nhu `Mon Apr ...`
- Tao `year`, `month`, `day`.
- Ghi vao `public.stg_frt_flexi_raw`.

ICC:

- Cast `used_duration` ve double.
- Parse `call_sta_time` theo:
  - `dd/MM/yyyy HH:mm:ss`
  - `yyyy-MM-dd`
  - `yyyyMMdd`
- Tao `year`, `month`, `day`.
- Ghi vao `public.stg_frt_in_icc_raw`.

### Buoc 8: Build aggregate Flexi daily

`build_flexi_daily()` tao dataset trung gian:

```text
usage_date
call_type_code
event_count
total_used_duration
```

Mapping:

- `usage_date` lay tu `record_opening_time`.
- `call_type_code` lay tu `record_type`, trim + uppercase, blank thanh `UNKNOWN`.
- `event_count` la so record Flexi trong nhom `(usage_date, call_type_code)`.
- `total_used_duration` la tong `duration` sau khi parse ve double.

Neu `ENABLE_SALT_AGG=true`, Spark dung salted 2-phase aggregation de giam skew khi mot key qua lon. Mac dinh dang tat (`false`) de tranh them shuffle khong can thiet.

### Buoc 9: Build dimension

`dwh.dim_date`:

- Lay cac `usage_date` moi tu Flexi daily.
- Tao `date_key = yyyyMMdd`.
- Tao cac thuoc tinh ngay/thang/quy/nam/week/weekend.
- Chi append ngay chua ton tai.

`dwh.dim_call_type`:

- Lay cac `call_type_code` moi tu Flexi daily.
- `call_type_name` bang chinh `call_type_code`.
- `is_active = true`.
- Chi append call type chua ton tai.

### Buoc 10: Build fact

Spark doc subset `dwh.dim_call_type` ung voi cac call type trong batch, join de lay `call_type_key`, roi tao `dwh.fact_usage_daily`.

Output fact:

| Cot | Gia tri |
|---|---|
| `date_key` | `yyyyMMdd` cua `usage_date`. |
| `call_type_key` | Surrogate key tu `dwh.dim_call_type`. |
| `usage_date` | Ngay su dung. |
| `call_type` | Code loai usage. |
| `event_count` | So record Flexi trong ngay + call type. |
| `total_used_duration` | Tong duration trong ngay + call type. |

Fact cung duoc ghi ra MinIO curated:

```text
s3a://datalake/curated/fact_usage_daily/
```

### Buoc 11: Build summary de API doc nhanh

`dwh.usage_summary_daily` la bang summary theo ngay va call type. Truoc khi ghi summary moi, Spark xoa cac row summary cua nhung `usage_date` trong batch, sau do append lai.

Cach nay giup summary cua mot ngay duoc refresh theo batch hien tai, tranh duplicate trong bang summary.

### Buoc 12: Cap nhat manifest

Neu thanh cong:

- Cac manifest row hop le duoc cap nhat `processed_flag = 1`.

Neu loi:

- Cac manifest row trong batch duoc cap nhat `processed_flag = 2`.
- `error_message` luu noi dung loi.

## 7. API chinh

| Endpoint | Muc dich |
|---|---|
| `GET /health` | Kiem tra API va PostgreSQL. |
| `GET /api/usage/daily` | Doc usage daily tu `dwh.fact_usage_daily` join `dwh.dim_call_type`. |
| `GET /api/analytics/metrics/usage-summary` | Tong hop event/duration theo call type tu `dwh.usage_summary_daily`. |
| `GET /api/analytics/metrics/usage-trend` | Trend theo ngay/thang tu `dwh.usage_summary_daily`. |
| `GET /internal/staging/flexi` | Xem staging Flexi. |
| `GET /internal/staging/icc` | Xem staging ICC. |

Vi du:

```bash
curl "http://localhost:8000/api/usage/daily?year=2026&month=4&limit=50"
curl "http://localhost:8000/api/analytics/metrics/usage-summary?from=2026-04-01&to=2026-04-30"
curl "http://localhost:8000/api/analytics/metrics/usage-trend?from=2026-04-01&to=2026-04-30&grain=day"
```

## 8. Cach chay local

### 8.1. Yeu cau

- Docker + Docker Compose v2.
- File `.env` o root repo.
- Thu muc input CSV tren host duoc mount vao NiFi:

```text
/mnt/d/data/Nifi-input -> /data/input trong container NiFi
```

Neu ban chay tren Windows/WSL, hay dam bao duong dan `/mnt/d/data/Nifi-input` ton tai va NiFi co the doc file trong do.

### 8.2. Start stack

```bash
docker compose up -d --build
```

Kiem tra:

```bash
docker compose ps
```

Chay them Metabase:

```bash
docker compose --profile bi up -d
```

### 8.3. Chay Spark ETL

Khuyen nghi dung script runner vi no doc `.env`, forward bien moi truong vao `docker compose exec`, co retry va log stage runtime.

Windows:

```bash
py scripts/run_spark_job.py
```

Linux/macOS/WSL:

```bash
python3 scripts/run_spark_job.py
```

Chay truc tiep trong container:

```bash
docker compose exec -T spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/project/spark/jobs/transform.py
```

## 9. Service URLs

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

`http://localhost:4040` chi co khi Spark job dang chay. Neu cong 4040 ban, Spark co the dung 4041, 4042, ...

## 10. Bien moi truong quan trong

| Bien | Y nghia |
|---|---|
| `DATALAKE_BUCKET` | Bucket MinIO, mac dinh `datalake`. |
| `MINIO_ENDPOINT` | Endpoint MinIO noi bo container, mac dinh `http://minio:9000`. |
| `PG_JDBC_URL` | JDBC URL den PostgreSQL. |
| `INGEST_MANIFEST_JOB_NAME` | Job name dung de claim manifest. |
| `INGEST_MANIFEST_BATCH_SIZE` | So file toi da Spark claim moi lan chay. |
| `MINIO_WRITE_MODE` | Mode ghi curated Parquet, vi du `append` hoac `overwrite`. |
| `STG_WRITE_MODE` | Mode ghi staging PostgreSQL. |
| `DWH_FACT_WRITE_MODE` | Mode ghi fact PostgreSQL. |
| `SPARK_SHUFFLE_PARTITIONS` | So partition khi shuffle. |
| `JDBC_BATCH_SIZE` | Batch size khi Spark ghi JDBC. |
| `JDBC_NUM_PARTITIONS` | So partition song song khi ghi JDBC. |
| `ENABLE_SALT_AGG` | Bat/tat salted aggregation cho key skew. |
| `CALL_TYPE_BROADCAST_THRESHOLD` | Nguong broadcast dimension call type. |

## 11. Lenh kiem tra du lieu

Kiem tra manifest:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT processed_flag, count(*)
FROM public.ingest_manifest
WHERE job_name='transform_usage_daily'
GROUP BY processed_flag
ORDER BY processed_flag;
"
```

Xem file dang cho xu ly:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT id, s3_path, ingest_time
FROM public.ingest_manifest
WHERE job_name='transform_usage_daily'
  AND processed_flag=0
ORDER BY ingest_time, id
LIMIT 20;
"
```

Kiem tra row count staging va DWH:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT count(*) AS stg_flexi FROM public.stg_frt_flexi_raw;
SELECT count(*) AS stg_icc FROM public.stg_frt_in_icc_raw;
SELECT count(*) AS dim_date FROM dwh.dim_date;
SELECT count(*) AS dim_call_type FROM dwh.dim_call_type;
SELECT count(*) AS fact_usage_daily FROM dwh.fact_usage_daily;
SELECT count(*) AS usage_summary_daily FROM dwh.usage_summary_daily;
"
```

Xem summary theo ngay:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
SELECT usage_date, call_type_code, event_count, total_used_duration
FROM dwh.usage_summary_daily
ORDER BY usage_date DESC, call_type_code
LIMIT 50;
"
```

## 12. Re-run va recovery

Reset manifest de replay toan bo file da ingest:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
UPDATE public.ingest_manifest
SET processed_flag=0,
    processed_time=NULL,
    batch_id=NULL,
    error_message=NULL
WHERE job_name='transform_usage_daily';
"
```

Replay nhung file loi:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
UPDATE public.ingest_manifest
SET processed_flag=0,
    processed_time=NULL,
    batch_id=NULL,
    error_message=NULL
WHERE job_name='transform_usage_daily'
  AND processed_flag=2;
"
```

Truncate staging va marts neu can rebuild sach:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c "
TRUNCATE TABLE
  public.stg_frt_flexi_raw,
  public.stg_frt_in_icc_raw,
  dwh.fact_usage_daily,
  dwh.usage_summary_daily;
"
```

Luu y: lenh truncate khong xoa `dwh.dim_date` va `dwh.dim_call_type`. Neu ban muon rebuild ca dimension, can can nhac foreign key va thu tu truncate rieng.

## 13. Cach hieu du lieu trong du an

Neu nhin theo goc nhin nghiep vu, du lieu trong du an la du lieu usage/cuoc goi dang event-level:

- Moi row Flexi la mot usage/charging record.
- `record_opening_time` cho biet usage xay ra khi nao.
- `record_type` cho biet loai usage.
- `duration` cho biet thoi luong su dung.
- `served_msisdn` giup trace usage ve thue bao.

Spark khong giu moi row Flexi trong fact. Spark gom cac row lai theo:

```text
usage_date + call_type_code
```

Va tinh:

```text
event_count = so record trong nhom
total_used_duration = tong duration trong nhom
```

Vi du neu trong Flexi co 3 record:

| record_opening_time | record_type | duration |
|---|---|---|
| 2026-04-01 08:00:00 | voice | 60 |
| 2026-04-01 09:00:00 | voice | 30 |
| 2026-04-01 10:00:00 | sms | 0 |

Sau ETL se thanh:

| usage_date | call_type_code | event_count | total_used_duration |
|---|---|---:|---:|
| 2026-04-01 | VOICE | 2 | 90 |
| 2026-04-01 | SMS | 1 | 0 |

Day la du lieu dashboard/API dang doc.

## 14. Nhung diem can nho

- `ingest_manifest` la bo nho trang thai file. Neu file khong co trong manifest, Spark se khong xu ly.
- `processed_flag=0` la chua xu ly, `9` la dang claim, `1` la thanh cong, `2` la loi.
- Flexi la nguon tao fact hien tai.
- ICC moi chi duoc luu staging, chua duoc merge vao fact.
- Summary table duoc refresh theo ngay trong batch de API doc nhanh hon.
- Neu chay lai Spark voi `DWH_FACT_WRITE_MODE=append`, fact co the bi duplicate neu replay manifest ma khong truncate/rebuild phan fact tuong ung. Summary co delete theo ngay truoc khi append nen it rui ro duplicate hon.
