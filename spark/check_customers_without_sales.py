from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ShopFlow Customers Without Sales")
    .master("local[*]")
    .getOrCreate()
)

customers = spark.read.parquet(
    "data/warehouse/dim_customer"
)

fact = spark.read.parquet(
    "data/warehouse/fact_orders"
)

customers_with_sales = (
    fact
    .select("customer_key")
    .distinct()
)

customers_without_sales = customers.join(
    customers_with_sales,
    customers.customer_key == customers_with_sales.customer_key,
    "left_anti"
)

customers_without_sales.select(
    "customer_key",
    "customer_id",
    "first_name",
    "last_name"
).show(truncate=False)

print(
    "Customers without sales:",
    customers_without_sales.count()
)

spark.stop()