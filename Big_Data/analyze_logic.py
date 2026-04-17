from pyspark.sql.functions import col, floor

def calculate_risk_by_grade(df):
    # Logic biểu đồ 1
    return df.groupBy("grade", "prediction").count()

def calculate_balance_dist(df):
    # Logic biểu đồ 4: Tính khoảng số dư
    return df.withColumn("balance_range", (floor(col("balance") / 5000) * 5000).cast("string")) \
             .groupBy("balance_range", "prediction").count()

def calculate_home_stats(df):
    # Logic biểu đồ 5
    return df.groupBy("home_ownership").count()

def calculate_edu_stats(df):
    # Logic biểu đồ 6
    return df.groupBy("education", "y").count()