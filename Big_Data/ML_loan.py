









# import os
# import uuid
# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, when, expr, monotonically_increasing_id
# from pyspark.sql.types import StructType, StructField, StringType
# from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.sql import SparkSession

# # Khởi tạo Spark hỗ trợ Delta và Kafka
# spark = SparkSession.builder \
#     .appName("consumer_loan") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

# # Cấu hình đường dẫn Docker (Mount từ D:/Big_Data)
# BASE_PATH = "/home/jovyan/work/output"
# checkpoint_path_final = f"{BASE_PATH}/checkpoints/loan"
# bronze_path = f"{BASE_PATH}/loan_bronze"
# silver_path = f"{BASE_PATH}/loan_silver"
# gold_path = f"{BASE_PATH}/loan_gold"
# ml_temp_path = f"{BASE_PATH}/ml_temp"

# # Cấu hình Kafka Cloud
# kafka_options = {
#     "kafka.bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
#     "kafka.security.protocol": "SASL_SSL",
#     "kafka.sasl.mechanism": "PLAIN",
#     "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="ZWSEEXNPTALGFSAS" password="cfltHleM7FMu1XNTdcfRK34pwFF8GbCoO/JGkWCevJXYqCjp5MMhJPvsXp62e+tw";'
# }

# #CELL 2
# print("--- Đang xử lý tầng Silver ---")
# df_bronze = spark.read.format("delta").load(bronze_path)

# json_schema = StructType([
#     StructField("loan_status", StringType(), True),
#     StructField("loan_amnt", StringType(), True),
#     StructField("int_rate", StringType(), True),
#     StructField("installment", StringType(), True),
#     StructField("annual_inc", StringType(), True),
#     StructField("dti", StringType(), True),
#     StructField("fico_range_low", StringType(), True),
#     StructField("term", StringType(), True),
#     StructField("grade", StringType(), True),
#     StructField("emp_length", StringType(), True),
#     StructField("home_ownership", StringType(), True)
# ])

# df_parsed = df_bronze.withColumn("jsonData", from_json(col("value"), json_schema)).select("jsonData.*")

# df_silver = df_parsed.filter(col("loan_status").isin(["Fully Paid", "Charged Off"])) \
#     .withColumn("label", when(col("loan_status") == "Fully Paid", 0.0).otherwise(1.0))

# numeric_cols = ["loan_amnt", "int_rate", "installment", "annual_inc", "dti", "fico_range_low"]
# for c in numeric_cols:
#     df_silver = df_silver.withColumn(c, expr(f"try_cast({c} as double)"))

# df_silver = df_silver.dropna(subset=["loan_amnt", "annual_inc"]).na.fill(0.0, subset=numeric_cols).na.fill("Unknown")

# df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
# print(f"✅ Silver hoàn tất! {df_silver.count()} dòng.")




# #CELL 3
# print("--- Đang tạo tầng Gold ---")
# df_silver_load = spark.read.format("delta").load(silver_path)
# total_count = df_silver_load.count()
# bad_count = df_silver_load.filter(col("label") == 1).count()
# weight_ratio = (total_count - bad_count) / bad_count

# df_gold_raw = df_silver_load.withColumn("classWeight", when(col("label") == 1, weight_ratio).otherwise(1.0))

# categorical_cols = ["term", "grade", "emp_length", "home_ownership"]
# indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in categorical_cols]
# assembler = VectorAssembler(inputCols=numeric_cols + [c + "_idx" for c in categorical_cols], outputCol="unscaled_features", handleInvalid="skip")
# scaler = StandardScaler(inputCol="unscaled_features", outputCol="features", withStd=True, withMean=False)

# pipeline = Pipeline(stages=indexers + [assembler] + [scaler])
# pipeline_model = pipeline.fit(df_gold_raw)
# data_final = pipeline_model.transform(df_gold_raw)

# data_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
# print(f"✅ Gold hoàn tất! Trọng số: {weight_ratio:.2f}")




# #CELL 4
# print("--- Đang huấn luyện mô hình ---")
# # Thay thế dbutils bằng os.makedirs
# os.makedirs(ml_temp_path, exist_ok=True)
# os.environ['SPARKML_TEMP_DFS_PATH'] = ml_temp_path

# train_data, test_data = spark.read.format("delta").load(gold_path).randomSplit([0.8, 0.2], seed=42)

# # Random Forest
# rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)
# rf_model = rf.fit(train_data)

# # Logistic Regression với CrossValidator
# lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="classWeight")
# paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
# cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, evaluator=BinaryClassificationEvaluator(), numFolds=3)
# cv_model = cv.fit(train_data)

# print("✅ Huấn luyện xong!")




# #CELL 5 

# def print_metrics(predictions, model_name):
#     evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
#     acc = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
#     auc = BinaryClassificationEvaluator().evaluate(predictions)
#     print(f"📊 {model_name} -> Accuracy: {acc:.4f}, AUC: {auc:.4f}")

# rf_preds = rf_model.transform(test_data)
# lr_preds = cv_model.bestModel.transform(test_data)

# print_metrics(rf_preds, "RANDOM FOREST")
# print_metrics(lr_preds, "LOGISTIC REGRESSION")

# # Lưu kết quả cuối cùng
# lr_preds.select("loan_amnt", "prediction", "label", "grade", "dti") \
#     .write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .save(f"{BASE_PATH}/loan_results")
# print("✅ Đã lưu kết quả Loan với đầy đủ các cột Grade và DTI.")
# print(f"🚀 TẤT CẢ ĐÃ XONG! Kết quả lưu tại D:/Big_Data/output/loan_results")






import os
import uuid
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, expr, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# 1. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("ML_Loan_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Cấu hình đường dẫn Docker
BASE_PATH = "/home/jovyan/work/output"
bronze_path = f"{BASE_PATH}/loan_bronze"
silver_path = f"{BASE_PATH}/loan_silver"
gold_path = f"{BASE_PATH}/loan_gold"
ml_temp_path = f"{BASE_PATH}/ml_temp"

# --- CELL 2: XỬ LÝ SILVER ---
print("--- Đang xử lý tầng Silver Loan ---")
df_bronze = spark.read.format("delta").load(bronze_path)

json_schema = StructType([
    StructField("loan_status", StringType(), True),
    StructField("loan_amnt", StringType(), True),
    StructField("int_rate", StringType(), True),
    StructField("installment", StringType(), True),
    StructField("annual_inc", StringType(), True),
    StructField("dti", StringType(), True),
    StructField("fico_range_low", StringType(), True),
    StructField("term", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True)
])

df_parsed = df_bronze.withColumn("jsonData", from_json(col("value"), json_schema)).select("jsonData.*")

# Lọc dữ liệu và gán nhãn label
df_silver = df_parsed.filter(col("loan_status").isin(["Fully Paid", "Charged Off"])) \
    .withColumn("label", when(col("loan_status") == "Fully Paid", 0.0).otherwise(1.0))

# Safe Casting các cột số
numeric_cols = ["loan_amnt", "int_rate", "installment", "annual_inc", "dti", "fico_range_low"]
for c in numeric_cols:
    df_silver = df_silver.withColumn(c, expr(f"try_cast({c} as double)"))

# Xử lý NULL
df_silver = df_silver.dropna(subset=["loan_amnt", "annual_inc"]).na.fill(0.0, subset=numeric_cols).na.fill("Unknown")

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
print(f"✅ Silver hoàn tất! {df_silver.count()} dòng.")

# --- CELL 3: TẠO TẦNG GOLD (VECTOR HÓA) ---
print("--- Đang tạo tầng Gold Loan ---")
df_silver_load = spark.read.format("delta").load(silver_path)

# Tính toán trọng số để cân bằng tập dữ liệu
total_count = df_silver_load.count()
bad_count = df_silver_load.filter(col("label") == 1).count()
weight_ratio = (total_count - bad_count) / bad_count
df_gold_raw = df_silver_load.withColumn("classWeight", when(col("label") == 1, weight_ratio).otherwise(1.0))

# Feature Engineering
categorical_cols = ["term", "grade", "emp_length", "home_ownership"]
indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in categorical_cols]
assembler = VectorAssembler(inputCols=numeric_cols + [c + "_idx" for c in categorical_cols], outputCol="unscaled_features", handleInvalid="skip")
scaler = StandardScaler(inputCol="unscaled_features", outputCol="features", withStd=True, withMean=False)

pipeline = Pipeline(stages=indexers + [assembler] + [scaler])
pipeline_model = pipeline.fit(df_gold_raw)
data_final = pipeline_model.transform(df_gold_raw)

data_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
print(f"✅ Gold hoàn tất! Trọng số rủi ro: {weight_ratio:.2f}")

# --- CELL 4: HUẤN LUYỆN MÔ HÌNH (CÓ CACHE) ---
print("--- Đang huấn luyện mô hình ML ---")
os.makedirs(ml_temp_path, exist_ok=True)
os.environ['SPARKML_TEMP_DFS_PATH'] = ml_temp_path

train_data, test_data = spark.read.format("delta").load(gold_path).randomSplit([0.8, 0.2], seed=42)

# 🚀 TỐI ƯU HÓA: Caching tập train để tăng tốc Random Forest
train_data.cache()
print(f"🔥 Đã đưa {train_data.count()} dòng vào RAM...")

# Random Forest
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)
rf_model = rf.fit(train_data)

# Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="classWeight")
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, evaluator=BinaryClassificationEvaluator(), numFolds=3)
cv_model = cv.fit(train_data)

# Giải phóng RAM
train_data.unpersist()
print("✅ Huấn luyện hoàn tất!")

# --- CELL 5: ĐÁNH GIÁ VÀ LƯU KẾT QUẢ ---
def evaluate_full_metrics(predictions, model_name):
    # 1. Bộ đánh giá đa lớp (cho Acc, Precision, Recall)
    multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    
    # 2. Bộ đánh giá nhị phân (cho AUC)
    binary_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")

    # Tính toán từng cái
    acc = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    prec = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    rec = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    auc = binary_evaluator.evaluate(predictions)

    print(f"\n📊 CHỈ SỐ CHI TIẾT CỦA {model_name}:")
    print(f"------------------------------------")
    print(f"- Accuracy (Độ chính xác):  {acc:.4f}")
    print(f"- Precision (Độ chính xác dự báo): {prec:.4f}")
    print(f"- Recall (Độ phủ/Nhạy):    {rec:.4f}")
    print(f"- AUC (Khả năng phân loại): {auc:.4f}")
    print(f"------------------------------------")

# Thực hiện dự báo và in kết quả cho cả 2 model
rf_preds = rf_model.transform(test_data)
lr_preds = cv_model.bestModel.transform(test_data)

evaluate_full_metrics(rf_preds, "RANDOM FOREST")
evaluate_full_metrics(lr_preds, "LOGISTIC REGRESSION")

# Lưu kết quả đầy đủ cột để Analyze.py không bị lỗi
lr_preds.select("loan_amnt", "prediction", "label", "grade", "dti") \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{BASE_PATH}/loan_results")

print(f"🚀 XONG! Kết quả lưu tại D:/Big_Data/output/loan_results")