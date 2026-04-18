from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Build_Silver").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def load_silver():
    clean_orders()
    clean_payments()
    clean_customers()
    clean_Items()

def clean_orders():
    df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_orders_dataset")
    df_before = df.count()

    df = df.dropna(subset=["order_id", "customer_id"])
    df = df.dropDuplicates(["order_id"])
    df = df.filter(col("order_status") == "delivered")

    df_after = df.count()
    print(f"  Order: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/silver_olist_orders_dataset")

def clean_payments():
    df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_order_payments_dataset")
    df_before = df.count()

    df = df.dropna(subset = ["order_id", "payment_sequential", "payment_value"])
    df = df.filter(col("payment_value") > 0)

    df_after = df.count()
    print(f"  Payments: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/silver_olist_order_payments_dataset")

def clean_Items():
    df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_order_items_dataset")

    df_before = df.count()

    df = df.dropna(subset = ["order_id", "order_item_id", "seller_id", "price"])
    df = df.filter(col("price") > 0)

    df_after = df.count()
    print(f"  Items: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/silver_olist_order_items_dataset")

def clean_customers():
    df: DataFrame = spark.read.parquet("/opt/spark-data/bronze/olist_customers_dataset")

    df_before = df.count()

    df = df.dropna(subset = ["customer_id", "customer_unique_id", "customer_zip_code_prefix"])
    df = df.dropDuplicates(["customer_id"])

    df_after = df.count()
    print(f"  Customers: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/silver_olist_customers_dataset")

if __name__ == "__main__":
    load_silver()

spark.stop()