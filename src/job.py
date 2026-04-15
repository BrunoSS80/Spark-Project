from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import time


spark = SparkSession.builder \
    .appName("MyJob") \
    .master("local[*]") \
    .getOrCreate()

df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_customers_dataset")

df.dropna

spark.stop()