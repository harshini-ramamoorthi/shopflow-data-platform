from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("ShopFlow Payment Transformation")
    .master("local[*]")
    .getOrCreate()
)

payment_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("payment_date", TimestampType(), True)
])

input_path = "data/raw/payments.csv"

df = (
    spark.read
    .option("header", True)
    .schema(payment_schema)
    .csv(input_path)
)

df = (
    df
    .withColumn("payment_method", lower(trim(col("payment_method"))))
    .withColumn("payment_status", lower(trim(col("payment_status"))))
)

df = df.dropDuplicates(["payment_id"])

df.printSchema()
df.show(10, truncate=False)

print("Payment rows:", df.count())

output_path = "data/staging/payments"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Payment staging transformation completed.")

spark.stop()