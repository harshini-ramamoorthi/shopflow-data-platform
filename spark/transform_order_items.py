from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DecimalType
)

spark = (
    SparkSession.builder
    .appName("ShopFlow Order Item Transformation")
    .master("local[*]")
    .getOrCreate()
)

order_item_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("discount", DecimalType(10, 2), True)
])

input_path = "data/raw/order_items.csv"

df = (
    spark.read
    .option("header", True)
    .schema(order_item_schema)
    .csv(input_path)
)

df = df.dropDuplicates(["order_item_id"])

df = df.withColumn(
    "line_total",
    (col("quantity") * col("unit_price")) - col("discount")
)

df.printSchema()
df.show(10, truncate=False)

print("Order item rows:", df.count())

output_path = "data/staging/order_items"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Order item staging transformation completed.")

spark.stop()