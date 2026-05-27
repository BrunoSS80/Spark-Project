from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count, sum as _sum, first, when
from pyspark.sql.window import Window
import pandas as pd
import os

spark = SparkSession.builder.appName("Build_Gold").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def load_gold():
    print("-----Carregando Camada Gold-----")
    gold_customers()
    gold_orders()
    gold_orders_items()
    gold_payments()
    load_master()

def gold_customers():
    print("-----Gerando Gold_Customers-----")
    df = spark.read.parquet("/opt/spark-data/silver/customers_clean")

    df = df.withColumn("customer_key", row_number().over(Window.orderBy(col("customer_id"))))

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/dim_customers")
    

def gold_orders():
    print("-----Gerando Gold_Orders-----")
    df = spark.read.parquet("/opt/spark-data/silver/orders_clean")

    df = df.withColumn("order_key", row_number().over(Window.orderBy(col("order_id"))))

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/dim_orders")

def gold_orders_items():
    print("-----Gerando Gold_Items-----")
    df = spark.read.parquet("/opt/spark-data/silver/orders_items_clean")

    df.write.mode("overwrite").parquet("/opt/spark-data/gold/fact_orders_items")

def gold_payments():
    print("-----Gerando Gold_Payments-----")
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

def load_master():
    print("-----Gerando Master (para relatórtios)-----")
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
                            .when(col("total_payment") < 200, "medium")
                            .when(col("total_payment") < 500, "high")
                            .otherwise("premium"))
                
    master.toPandas().to_parquet("/opt/spark-data/gold/master_sales.parquet")

    os.system("streamlit run /opt/spark-src/dash.py")

if __name__ == "__main__":
    load_gold()
