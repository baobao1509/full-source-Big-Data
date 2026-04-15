import os
import time
from pyspark.sql import SparkSession

# 1. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Demo_Realtime_Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. ĐƯỜNG DẪN VÀ CẤU HÌNH
BASE_PATH = "/home/jovyan/work/output"
bronze_path = f"{BASE_PATH}/loan_bronze"
checkpoint_path = f"{BASE_PATH}/checkpoints/demo_loan"

kafka_options = {
    "kafka.bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="ZWSEEXNPTALGFSAS" password="cfltHleM7FMu1XNTdcfRK34pwFF8GbCoO/JGkWCevJXYqCjp5MMhJPvsXp62e+tw";'
}

# 3. HÀM XỬ LÝ MỖI KHI CÓ DATA MỚI (Thay thế cho run_streaming_batch)
def process_and_count(batch_df, batch_id):
    if batch_df.count() > 0:
        # Ghi vào Bronze
        batch_df.write.format("delta").mode("append").save(bronze_path)
        
        # Đọc lại toàn bộ kho để đếm tổng số
        total_count = spark.read.format("delta").load(bronze_path).count()
        
        print(f"🔔 [Lô mới!] Nhận thêm: {batch_df.count()} dòng.")
        print(f"📊 TỔNG CỘNG TRONG KHO BRONZE: {total_count} dòng.")
        print("-" * 40)
    else:
        print(f"💤 Đang đợi dữ liệu từ Kafka... ({time.strftime('%H:%M:%S')})")

# 4. BẬT LUỒNG STREAMING CHẠY VÔ TẬN
print("🚀 ĐANG BẬT LUỒNG HỨNG DỮ LIỆU REAL-TIME...")

df_kafka_raw = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", "loan_transaction") \
    .option("startingOffsets", "earliest") \
    .load()

query = df_kafka_raw.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .foreachBatch(process_and_count) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='5 seconds') \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("🛑 Đã dừng luồng.")

