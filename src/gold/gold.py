from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Build_Gold").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/opt/spark-data/silver/silver_olist_order_items_dataset")

df.printSchema()
spark.stop()