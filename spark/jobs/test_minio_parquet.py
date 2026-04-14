from pyspark.sql import SparkSession

# ===== CONFIG =====
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "12345678"
BUCKET = "datalake"

# 👉 sửa path theo bạn
PATH = "s3a://datalake/raw/"

# ===== INIT SPARK =====
spark = SparkSession.builder \
    .appName("Test MinIO Parquet") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ===== TEST READ =====
print("🔍 Reading data from MinIO...")

df = spark.read.parquet(PATH)

# ===== SHOW INFO =====
print("\n📊 Schema:")
df.printSchema()

print("\n🔢 Row count:")
print(df.count())

print("\n📌 Sample data:")
df.show(5, truncate=False)

# ===== EXTRA DEBUG =====
print("\n📁 Files:")
files = df.inputFiles()
for f in files:
    print(f)

spark.stop()
