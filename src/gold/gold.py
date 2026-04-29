from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, substring, row_number,count_distinct, count, sum as _sum, first, when
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("Build_Gold").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def gold_customers():
    df = spark.read.parquet("/opt/spark-data/silver/customers_clean")

    df = df.withColumn("customer_key", row_number().over(Window.orderBy(col("customer_id"))))

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/dim_customers")

def gold_orders():
    df = spark.read.parquet("/opt/spark-data/silver/orders_clean")

    df = df.withColumn("order_key", row_number().over(Window.orderBy(col("order_id"))))

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/dim_orders")

def gold_orders_items():
    df = spark.read.parquet("/opt/spark-data/silver/orders_items_clean")

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/fact_orders_items")

def gold_payments():
    df = spark.read.parquet("/opt/spark-data/silver/orders_payments_clean")

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/fact_orders_payments")

def payments_aggregation(df):
    return df.groupBy("order_id").agg(
    _sum(col("payment_value")).alias("total_payment"),
    count(col("payment_sequential")).alias("payment_count"),
    first(col("payment_type")).alias("principal_payment_type")
    )

def items_aggregation(df):
    return df.groupBy("order_id").agg(
    _sum(col("price")).alias("value_items"),
    _sum(col("freight_value")).alias("total_freight"),
    count(col("order_item_id")).alias("count_items")
    )

customers = spark.read.parquet("/opt/spark-data/gold/dim_customers")
orders = spark.read.parquet("/opt/spark-data/gold/dim_orders")
items = spark.read.parquet("/opt/spark-data/gold/fact_orders_items")
payments = spark.read.parquet("/opt/spark-data/gold/fact_orders_payments")

payments_agg = payments_aggregation(payments)

items_agg = items_aggregation(items)

master = items_agg.join(orders, "order_id")\
            .join(customers, "customer_id")\
            .join(payments_agg, "order_id")

master = master.withColumn("order_value_bucket",\
                           when(col("total_payment") < 50, "low")
                           .when(col("total_payment") < 100, "medium")
                           .when(col("total_payment") < 150, "high")
                           .otherwise("premium")).show()
            
#            .groupBy(
#                "customer_state",
#                "customer_city",
#                "principal_payment_type",
#                "order_purchase_date"
#            )\
#            .agg(
#                _sum(col("price").cast(DecimalType(10,2))).alias("total_revenue"),
#                _sum(col("freight_value").cast(DecimalType(10,2))).alias("total_freght"),
#                _sum(col("payment_value").cast(DecimalType(10,2))).alias("total_paid"),
#                count_distinct("order_id").alias("total_orders"),
#                count("order_item_id").alias("total_items")
#            ).show()


spark.stop()