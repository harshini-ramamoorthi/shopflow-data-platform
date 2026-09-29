from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, round, col

spark = (
    SparkSession.builder
    .appName("ShopFlow Customer Sales")
    .master("local[*]")
    .getOrCreate()
)

# Load warehouse tables
fact = spark.read.parquet("data/warehouse/fact_orders")
dim_customer = spark.read.parquet("data/warehouse/dim_customer")

# Create aliases to avoid ambiguous column references
fact_df = fact.alias("fact")
customer_df = dim_customer.alias("customer")

# Join fact table with customer dimension
customer_sales = fact_df.join(
    customer_df,
    col("fact.customer_key") == col("customer.customer_key"),
    "inner"
)

# Aggregate sales by customer
customer_sales = customer_sales.groupBy(
    col("customer.customer_key"),
    col("customer.customer_id"),
    col("customer.first_name"),
    col("customer.last_name"),
    col("customer.city"),
    col("customer.state"),
    col("customer.country")
).agg(
    countDistinct("fact.order_id").alias("total_orders"),
    sum("fact.quantity").alias("units_purchased"),
    round(sum("fact.revenue"), 2).alias("total_revenue")
)

# Sort customers by revenue
customer_sales = customer_sales.orderBy(
    "total_revenue",
    ascending=False
)

# Inspect the result
customer_sales.printSchema()

customer_sales.show(10, truncate=False)

print("Customer sales rows:", customer_sales.count())

# Save analytics output
output_path = "data/analytics/customer_sales"

(
    customer_sales.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Customer sales analytics created successfully.")

spark.stop()