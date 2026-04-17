import pytest
from pyspark.sql import SparkSession
from Big_Data.analyze_logic import calculate_balance_dist

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()

def test_calculate_balance_dist(spark):
    # 1. Tạo dữ liệu giả
    data = [(7000.0, "0.0"), (3000.0, "1.0")]
    schema = ["balance", "prediction"]
    df = spark.createDataFrame(data, schema)

    # 2. Chạy hàm logic
    result_df = calculate_balance_dist(df)
    results = result_df.collect()

    # 3. Lấy danh sách kết quả
    ranges = [r["balance_range"] for r in results]
    
    # SỬA LẠI Ở ĐÂY: Bỏ cái .0 đi vì Spark cast sang String thường mất phần thập phân nếu là .0
    assert "5000" in ranges
    assert "0" in ranges
    
    print("✅ Test logic phân dải số dư thành công!")