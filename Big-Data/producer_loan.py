from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json, monotonically_increasing_id
import time

# 1. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("Producer_Cloud") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 2. Cấu hình Kafka Cloud
kafka_options = {
    "kafka.bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="ZWSEEXNPTALGFSAS" password="cfltHleM7FMu1XNTdcfRK34pwFF8GbCoO/JGkWCevJXYqCjp5MMhJPvsXp62e+tw";'
}

# 3. Đọc và bắn dữ liệu LOAN
try:
    print("--- Đang đọc file LOAN từ ổ D ---")
    # Đường dẫn file CSV ở gốc ổ D
    df_loan = spark.read.option("header", "True").option("inferSchema", "True").csv("/data/accepted_2007_to_2018Q4.csv").limit(50000)
    df_loan = df_loan.withColumn("temp_id", monotonically_increasing_id())
    
    print("--- Bắt đầu bắn LOAN lên Cloud ---")
    for i in range(0, 5000, 1000):
        batch = df_loan.filter((col("temp_id") >= i) & (col("temp_id") < i + 1000)).drop("temp_id")
        batch.select(to_json(struct([col(c) for c in batch.columns])).alias("value")) \
            .write.format("kafka").options(**kafka_options).option("topic", "loan_transaction").save()
        print(f"✅ Đã gửi xong lô {i+1000} dòng.")
        time.sleep(10)

    print("🚀 THÀNH CÔNG RỰC RỠ!")
except Exception as e:
    print(f"❌ Lỗi: {e}")