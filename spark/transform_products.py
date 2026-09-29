from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap
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
    .appName("ShopFlow Product Transformation")
    .master("local[*]")
    .getOrCreate()
)

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("cost_price", DecimalType(10, 2), True),
    StructField("supplier", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

input_path = "data/raw/products.csv"

df = (
    spark.read
    .option("header", True)
    .schema(product_schema)
    .csv(input_path)
)

df = (
    df
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", initcap(trim(col("category"))))
    .withColumn("subcategory", initcap(trim(col("subcategory"))))
    .withColumn("supplier", trim(col("supplier")))
)

df = df.dropDuplicates(["product_id"])

df = df.withColumn(
    "profit_per_unit",
    col("unit_price") - col("cost_price")
)

df.printSchema()
df.show(10, truncate=False)

print("Product rows:", df.count())

output_path = "data/staging/products"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Product staging transformation completed.")

spark.stop()
