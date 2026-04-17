import os
import time
from pyspark.sql import SparkSession

# 1. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Demo_Marketing_Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. ĐƯỜNG DẪN VÀ CẤU HÌNH (Sửa lại đường dẫn Docker)
BASE_PATH = "/home/jovyan/work/output"
mkt_bronze_path = f"{BASE_PATH}/mkt_bronze"
checkpoint_path = f"{BASE_PATH}/checkpoints/mkt_demo"

kafka_options = {
    "kafka.bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="ZWSEEXNPTALGFSAS" password="cfltHleM7FMu1XNTdcfRK34pwFF8GbCoO/JGkWCevJXYqCjp5MMhJPvsXp62e+tw";'
}

# 3. HÀM XỬ LÝ MỖI KHI CÓ DATA MARKETING MỚI
def process_mkt_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        # Ghi vào Bronze
        batch_df.write.format("delta").mode("append").save(mkt_bronze_path)
        
        # Đọc lại toàn bộ kho để đếm tổng số
        total_count = spark.read.format("delta").load(mkt_bronze_path).count()
        
        print(f"📣 [Marketing] Nhận thêm: {batch_df.count()} dòng.")
        print(f"📊 TỔNG CỘNG TRONG KHO BRONZE MKT: {total_count} dòng.")
        print("-" * 40)
    else:
        print(f"💤 Đang đợi dữ liệu Marketing... ({time.strftime('%H:%M:%S')})")

# 4. BẬT LUỒNG STREAMING
print("🚀 ĐANG BẬT LUỒNG HỨNG DỮ LIỆU MARKETING REAL-TIME...")

df_mkt_raw = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", "bank_marketing") \
    .option("startingOffsets", "earliest") \
    .load()

query = df_mkt_raw.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .foreachBatch(process_mkt_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='5 seconds') \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("🛑 Đã dừng luồng Marketing.")