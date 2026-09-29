from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


spark = (
    SparkSession.builder
    .appName("ShopFlow Payment Dimension")
    .master("local[*]")
    .getOrCreate()
)


input_path = "data/staging/payments"

df = spark.read.parquet(input_path)


df = df.select(
    "payment_id",
    "payment_method",
    "order_id",
    "payment_status"
)


df = df.withColumn(
    "payment_key",
    monotonically_increasing_id()
)


df = df.select(
    "payment_key",
    "payment_id",
    "order_id",
    "payment_method",
    "payment_status"
)


df.printSchema()

df.show(10, truncate=False)

print("Payment dimension rows:", df.count())


output_path = "data/warehouse/dim_payment"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print("Payment dimension created successfully.")


spark.stop()