from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Build_Bronze").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

datasets = [
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset"
]

path_data = "/opt/spark-data/input"
path_load = "/opt/spark-data/bronze"

def load_bronze():
    try:
        for dataset in datasets:
            print(f"-----Lendo arquivo {dataset}-----")

            df = spark.read.csv(f"{path_data}/{dataset}.csv", header = True, inferSchema = True)
            df.show()
            print(f"-----Gravando arquivo {dataset} como parquet-----")
            df.write.mode("overwrite").parquet(f"{path_load}/{dataset}")

    except Exception as e:
        print(f"Algo deu errado, erro: {e}")

    else:
        print("-----PROCESSO CONCLUIDO-----")
        spark.stop()

if __name__ == "__main__":
    load_bronze()

spark.stop()