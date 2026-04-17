import pytest
from pyspark.sql import SparkSession
from Big_Data.analyze_logic import calculate_balance_dist
    
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()

def test_calculate_balance_dist(spark):
    # 1. Tạo dữ liệu giả: một người có 7000, một người có 3000
    data = [(7000.0, "0.0"), (3000.0, "1.0")]
    schema = ["balance", "prediction"]
    df = spark.createDataFrame(data, schema)

    # 2. Chạy hàm logic
    result_df = calculate_balance_dist(df)
    results = result_df.collect()

    # 3. Kiểm tra: 
    # 7000 / 5000 = 1.4 -> floor = 1 -> 1 * 5000 = "5000.0"
    # 3000 / 5000 = 0.6 -> floor = 0 -> 0 * 5000 = "0.0"
    ranges = [r["balance_range"] for r in results]
    assert "5000.0" in ranges
    assert "0.0" in ranges