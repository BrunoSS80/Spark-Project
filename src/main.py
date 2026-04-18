from pyspark.sql import SparkSession
from bronze.bronze import load_bronze
from silver.silver import load_silver

spark = SparkSession.builder.appName("Build_Bronze").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    load_bronze()
    load_silver()

spark.stop()