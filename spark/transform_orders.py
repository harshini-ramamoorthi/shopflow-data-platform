from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("ShopFlow Order Transformation")
    .master("local[*]")
    .getOrCreate()
)

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_status", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

input_path = "data/raw/orders.csv"

df = (
    spark.read
    .option("header", True)
    .schema(order_schema)
    .csv(input_path)
)

df = (
    df
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("shipping_city", trim(col("shipping_city")))
    .withColumn("shipping_state", trim(col("shipping_state")))
)

df = df.dropDuplicates(["order_id"])

df.printSchema()
df.show(10, truncate=False)

print("Order rows:", df.count())

output_path = "data/staging/orders"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Order staging transformation completed.")

spark.stop()