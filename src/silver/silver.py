from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number, substring
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Build_Silver").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def load_silver():
    print("-----Iniciando Camada Silver-----")
    clean_orders()
    clean_payments()
    clean_customers()
    clean_items()
    print("-----Camada Silver Concluida-----")

def clean_orders():
    df = spark.read.parquet("/opt/spark-data/bronze/olist_orders_dataset")
    df_before = df.count()

    df = df.dropna(subset=["order_id", "customer_id"])
    df = df.dropDuplicates(["order_id"])
    df = df.filter(col("order_status") == "delivered")

    df = df.select('order_id', 'customer_id', 'order_status', substring('order_purchase_timestamp', 1, 10).alias('order_purchase_date'),\
          substring('order_approved_at', 1, 10).alias('order_approved_at'),\
          substring('order_delivered_carrier_date',1,10).alias('order_delivered_carrier_date'),\
          substring('order_delivered_customer_date',1,10).alias('order_delivered_customer_date'),\
          substring('order_estimated_delivery_date',1,10).alias('order_estimated_delivery_date'))

    df_after = df.count()
    print(f"  Order: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/orders_clean")

def clean_payments():
    df = spark.read.parquet("/opt/spark-data/bronze/olist_order_payments_dataset")
    df_before = df.count()

    df = df.dropna(subset = ["order_id", "payment_sequential", "payment_value"])
    df = df.filter(col("payment_value") > 0)
    df = df.withColumn("payment_value", col("payment_value").cast(DecimalType(10,2)))

    df_after = df.count()
    print(f"  Payments: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/orders_payments_clean")

def clean_items():
    df = spark.read.parquet("/opt/spark-data/bronze/olist_order_items_dataset")

    df_before = df.count()

    df = df.dropna(subset = ["order_id", "order_item_id", "seller_id", "price"])
    df = df.filter(col("price") > 0)
    df = df.withColumn("row_num", row_number().over(Window.partitionBy("order_id", "order_item_id").orderBy(col("order_id"))))
    df = df.filter(col("row_num") == 1).orderBy(col("order_id")).drop("row_num")

    df = df.select('order_id', 'order_item_id', 'product_id', 'seller_id',\
          substring('shipping_limit_date',1,10).alias('shipping_limit_date'),\
          col('price').cast(DecimalType(10,2)), col('freight_value').cast(DecimalType(10,2)))
    
    df_after = df.count()
    print(f"  Items: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/orders_items_clean")

def clean_customers():
    df = spark.read.parquet("/opt/spark-data/bronze/olist_customers_dataset")

    df_before = df.count()

    df = df.dropna(subset = ["customer_id", "customer_unique_id", "customer_zip_code_prefix"])
    df = df.dropDuplicates(["customer_id"])

    df_after = df.count()
    print(f"  Customers: {df_before} --> {df_after}, foram ({df_before - df_after} linhas removidas)")
    df.write.mode("overwrite").parquet("/opt/spark-data/silver/customers_clean")

if __name__ == "__main__":
    load_silver()