# Dùng bản image gốc của Spark
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
# Cài thêm các thư viện Spark cần để chạy project của mày
RUN pip install delta-spark==3.0.0 mongo-spark-connector

# Copy toàn bộ code xử lý vào bên trong image
COPY . /home/jovyan/work/

WORKDIR /home/jovyan/work