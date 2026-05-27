from pyspark.sql import SparkSession
from bronze.bronze import load_bronze
from silver.silver import load_silver
from gold.gold import load_gold

spark = SparkSession.builder \
    .appName("Build_Data") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    load_bronze()
    load_silver()
    load_gold()
    spark.stop()
