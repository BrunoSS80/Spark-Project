from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName("Build_Bronze").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_customers_dataset")

df_before = df.count()

df.dropna(subset = ["customer_id", "customer_unique_id"])
df.dropDuplicates(["customer_id"])

df_after = df.count()

print(f"Antes: {df_before}, depois {df_after}, foram {df_before - df_after}")