from pyspark.sql import SparkSession
from bronze.bronze import load_bronze

spark = SparkSession.builder.appName("Build_Bronze").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    load_bronze()