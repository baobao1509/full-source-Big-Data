from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, from_json, schema_of_json

# 1. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Mkt_Surgery_Fix") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BASE_PATH = "/home/jovyan/work/output"
mkt_bronze_path = f"{BASE_PATH}/mkt_bronze"
mkt_silver_path = f"{BASE_PATH}/mkt_silver"
mkt_gold_path = f"{BASE_PATH}/mkt_gold"
mkt_results_path = f"{BASE_PATH}/marketing_results"

# 2. ĐỌC DỮ LIỆU TỪ BRONZE
df_bronze = spark.read.format("delta").load(mkt_bronze_path)

# 3. LẤY SCHEMA TỰ ĐỘNG (Nó sẽ ra 1 cái schema có 1 cột duy nhất cực dài)
sample_json = df_bronze.select("value").limit(1).collect()[0][0]
dynamic_schema = schema_of_json(sample_json)

# 4. PARSE JSON RA (Lúc này df chỉ có 1 cột duy nhất tên là cái header bùi nhùi kia)
df_messy = df_bronze.withColumn("data", from_json(col("value"), dynamic_schema)).select("data.*")
messy_col_name = df_messy.columns[0] # Tên cột quái thai

# 5. TÁCH DỮ LIỆU BẰNG DẤU CHẤM PHẨY (;)
# Định nghĩa danh sách cột chuẩn
column_names = ["age", "job", "marital", "education", "default", "balance", "housing", "loan", "contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y"]

# Tách cái cột duy nhất thành một mảng (Array)
df_split = df_messy.withColumn("split_data", split(col(f"`{messy_col_name}`"), ";"))

# Bốc từng phần tử trong mảng ra thành cột riêng và xóa dấu ngoặc kép (")
for i, name in enumerate(column_names):
    df_split = df_split.withColumn(name, regexp_replace(df_split["split_data"].getItem(i), '"', ''))

# Loại bỏ các cột trung gian
df_clean = df_split.drop(messy_col_name, "split_data")

# 6. ÉP KIỂU VÀ LÀM SẠCH SILVER
num_cols = ["age", "balance", "duration", "campaign"]
for c in num_cols:
    df_clean = df_clean.withColumn(c, col(c).cast("double"))

df_silver = df_clean.dropna(subset=["age", "job"]) \
    .withColumn("label", when(col("y") == "yes", 1.0).otherwise(0.0))

print(f"✅ PHẪU THUẬT THÀNH CÔNG! Số dòng sạch: {df_silver.count()}")

# 7. GHI XUỐNG SILVER VÀ CHẠY TIẾP GOLD/ML (Y hệt code cũ)
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(mkt_silver_path)

# ... (Phần code Gold và ML phía sau mày giữ nguyên như cũ nhé) ...

# --- [BƯỚC 2: XỬ LÝ GOLD - VECTOR HÓA] ---
print("--- Đang chuẩn bị dữ liệu dạng Vector cho tầng Gold ---")
df_silver_load = spark.read.format("delta").load(mkt_silver_path)

cat_cols = ["job", "marital", "education", "housing", "loan"]
indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in cat_cols]

# Gộp các cột thành cột 'features' dạng VECTOR
assembler = VectorAssembler(inputCols=num_cols + [c+"_idx" for c in cat_cols], 
                            outputCol="features", handleInvalid="skip")

# Dùng Pipeline để transform dữ liệu sang Gold
pipeline_gold = Pipeline(stages=indexers + [assembler])
df_gold = pipeline_gold.fit(df_silver_load).transform(df_silver_load)

# Lưu xuống Gold (Lúc này bảng sẽ có cột 'features' cực quan trọng)
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(mkt_gold_path)
print("🚀 Đã tạo xong tầng Gold (Dữ liệu đã ở dạng Vector)!")

# --- [BƯỚC 3: HUẤN LUYỆN ML] ---
print("--- Đang huấn luyện mô hình từ tầng Gold ---")
df_train_ready = spark.read.format("delta").load(mkt_gold_path)

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

train, test = df_train_ready.randomSplit([0.8, 0.2], seed=42)
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)
model = rf.fit(train)
predictions = model.transform(test)

# Đánh giá và lưu kết quả
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
print(f"📊 Accuracy: {evaluator.evaluate(predictions):.4f}")

predictions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(mkt_results_path)
print("🏁 TẤT CẢ ĐÃ HOÀN TẤT!")