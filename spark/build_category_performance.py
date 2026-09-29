from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, col

spark = (
    SparkSession.builder
    .appName("ShopFlow Category Performance")
    .master("local[*]")
    .getOrCreate()
)

# Load warehouse tables
fact = spark.read.parquet("data/warehouse/fact_orders")
dim_product = spark.read.parquet("data/warehouse/dim_product")

# Create aliases
fact_df = fact.alias("fact")
product_df = dim_product.alias("product")

# Join fact table with product dimension
category_performance = fact_df.join(
    product_df,
    col("fact.product_key") == col("product.product_key"),
    "inner"
)

# Aggregate sales by category
category_performance = category_performance.groupBy(
    col("product.category")
).agg(
    sum("fact.quantity").alias("units_sold"),
    round(sum("fact.revenue"), 2).alias("total_revenue")
)

# Sort categories by revenue
category_performance = category_performance.orderBy(
    "total_revenue",
    ascending=False
)

# Inspect results
category_performance.printSchema()

category_performance.show(
    truncate=False
)

print(
    "Category performance rows:",
    category_performance.count()
)

# Save analytics output
output_path = "data/analytics/category_performance"

(
    category_performance.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Category performance analytics created successfully.")

spark.stop()