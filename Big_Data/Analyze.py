from pyspark.sql import SparkSession
from analyze_logic import * # Nhớ là phải đổi tên folder thành big_data_code nhé

# 1. CẤU HÌNH ĐỊA CHỈ
atlas_uri = "mongodb+srv://giabaodongthanh:15092004@cluster0.j2vxi.mongodb.net/?appName=Cluster0"
local_uri = "mongodb://mongodb:27017" # Tên service trong mạng Docker

# 2. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Analyze_Dual_Write") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BASE_PATH = "/home/jovyan/work/output"

def save_dual(df, collection_name):
    print(f"--- Đang ghi collection: {collection_name} ---")
    # Ghi vào MongoDB Local (Docker)
    df.write.format("mongodb").mode("overwrite") \
      .option("database", "analyze") \
      .option("collection", collection_name) \
      .option("connection.uri", local_uri).save()
    
    # Ghi vào MongoDB Atlas (Cloud cho Vercel)
    df.write.format("mongodb").mode("overwrite") \
      .option("database", "analyze") \
      .option("collection", collection_name) \
      .option("connection.uri", atlas_uri).save()
    
    print(f"✅ Xong {collection_name}")

# --- THỰC THI PHÂN TÍCH ---
print("🚀 Bắt đầu quá trình Analyze...")

# Đọc dữ liệu nguồn
df_loan_res = spark.read.format("delta").load(f"{BASE_PATH}/loan_results")
df_mkt_res = spark.read.format("delta").load(f"{BASE_PATH}/marketing_results")
df_loan_silver = spark.read.format("delta").load(f"{BASE_PATH}/loan_silver")
df_mkt_silver = spark.read.format("delta").load(f"{BASE_PATH}/mkt_silver")

# Gọi các hàm từ analyze_logic và lưu đôi
save_dual(calculate_risk_by_grade(df_loan_res), "viz_loan_grade")
save_dual(calculate_balance_dist(df_mkt_res), "viz_mkt_balance")
save_dual(calculate_home_stats(df_loan_silver), "viz_loan_home")
save_dual(calculate_edu_stats(df_mkt_silver), "viz_mkt_edu")

# Hai bảng đơn giản ghi trực tiếp
save_dual(df_loan_res.select("dti", "loan_amnt", "prediction").limit(1000), "viz_loan_scatter")
save_dual(df_mkt_res.groupBy("job", "prediction").count(), "viz_mkt_job")

print("\n🏁 HOÀN TẤT! Dữ liệu đã đồng bộ Local Docker và Cloud Atlas.")