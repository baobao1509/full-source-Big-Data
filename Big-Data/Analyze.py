from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, floor

# 1. KHỞI TẠO SPARK VỚI MONGODB CONNECTOR
# Thêm package mongo-spark-connector để Spark biết ghi vào Mongo
spark = SparkSession.builder \
    .appName("Analyze_To_MongoDB") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
    .getOrCreate()

# Đường dẫn dữ liệu trong Docker
BASE_PATH = "/home/jovyan/work/output"
DB_NAME = "analyze"

# Hàm hỗ trợ ghi vào MongoDB cho gọn code
def save_to_mongo(df, collection_name):
    df.write.format("mongodb") \
      .mode("overwrite") \
      .option("database", DB_NAME) \
      .option("collection", collection_name) \
      .save()
    print(f"✅ Đã lưu xong collection: {collection_name}")

# --- BẮT ĐẦU PHÂN TÍCH ---

print("--- Đang tải dữ liệu kết quả từ Delta Lake ---")
df_loan_res = spark.read.format("delta").load(f"{BASE_PATH}/loan_results")
df_mkt_res = spark.read.format("delta").load(f"{BASE_PATH}/marketing_results")
df_loan_silver = spark.read.format("delta").load(f"{BASE_PATH}/loan_silver")
df_mkt_silver = spark.read.format("delta").load(f"{BASE_PATH}/mkt_silver")

# --- THỐNG KÊ CHO DASHBOARD ---

# 1. Phân bổ rủi ro theo Grade (Dataset 1)
risk_by_grade = df_loan_res.groupBy("grade", "prediction").count()
save_to_mongo(risk_by_grade, "viz_loan_grade")

# 2. DTI vs Loan Amount (Scatter Plot)
dti_amt = df_loan_res.select("dti", "loan_amnt", "prediction").limit(1000)
save_to_mongo(dti_amt, "viz_loan_scatter")

# 3. Chốt đơn theo Nghề nghiệp (Dataset 2)
job_conv = df_mkt_res.groupBy("job", "prediction").count() 
save_to_mongo(job_conv, "viz_mkt_job")

# 4. Phân bổ Số dư (Balance)
balance_dist = df_mkt_res.withColumn("balance_range", (floor(col("balance") / 5000) * 5000).cast("string")) \
    .groupBy("balance_range", "prediction").count()
save_to_mongo(balance_dist, "viz_mkt_balance")

# 5. Tình trạng nhà ở của người đi vay (Thực tế)
home_stats = df_loan_silver.groupBy("home_ownership").count()
save_to_mongo(home_stats, "viz_loan_home")

# 6. Tỷ lệ chốt đơn theo Học vấn (Thực tế)
edu_stats = df_mkt_silver.groupBy("education", "y").count()
save_to_mongo(edu_stats, "viz_mkt_edu")

print("\n🚀 TẤT CẢ BẢNG THỐNG KÊ ĐÃ NẰM TRONG MONGODB!")
print("Mày vào localhost:8081 xem database 'bank_dashboard' để kiểm tra nhé.")