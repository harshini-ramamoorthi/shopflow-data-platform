from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


spark = (
    SparkSession.builder
    .appName("ShopFlow Product Dimension")
    .master("local[*]")
    .getOrCreate()
)


input_path = "data/staging/products"

df = spark.read.parquet(input_path)


df = df.select(
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "unit_price",
    "cost_price",
    "profit_per_unit",
    "supplier"
)


df = df.withColumn(
    "product_key",
    monotonically_increasing_id()
)


df = df.select(
    "product_key",
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "unit_price",
    "cost_price",
    "profit_per_unit",
    "supplier"
)


df.printSchema()

df.show(10, truncate=False)

print("Product dimension rows:", df.count())


output_path = "data/warehouse/dim_product"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print("Product dimension created successfully.")


spark.stop()