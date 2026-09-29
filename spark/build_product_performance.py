from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = (
    SparkSession.builder
    .appName("ShopFlow Product Performance")
    .master("local[*]")
    .getOrCreate()
)

fact = spark.read.parquet("data/warehouse/fact_orders")
dim_product = spark.read.parquet("data/warehouse/dim_product")

fact_df = fact.alias("fact")
product_df = dim_product.alias("product")

product_performance = fact_df.join(
    product_df,
    col("fact.product_key") == col("product.product_key"),
    "inner"
)

product_performance = product_performance.groupBy(
    col("product.product_key"),
    col("product.product_id"),
    col("product.product_name"),
    col("product.category"),
    col("product.subcategory")
).agg(
    sum("fact.quantity").alias("units_sold"),
    round(sum("fact.revenue"), 2).alias("total_revenue")
)

product_performance = product_performance.orderBy(
    "total_revenue",
    ascending=False
)

product_performance.printSchema()

product_performance.show(10, truncate=False)

print("Product performance rows:", product_performance.count())

output_path = "data/analytics/product_performance"

(
    product_performance.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Product performance analytics created successfully.")

spark.stop()