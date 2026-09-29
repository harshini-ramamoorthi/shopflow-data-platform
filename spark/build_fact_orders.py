from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .appName("ShopFlow Fact Orders")
    .master("local[*]")
    .getOrCreate()
)

orders = spark.read.parquet("data/staging/orders")
order_items = spark.read.parquet("data/staging/order_items")

dim_customer = spark.read.parquet("data/warehouse/dim_customer")
dim_product = spark.read.parquet("data/warehouse/dim_product")
dim_date = spark.read.parquet("data/warehouse/dim_date")
dim_payment = spark.read.parquet("data/warehouse/dim_payment")

orders_df = orders.alias("orders")
order_items_df = order_items.alias("order_items")
customer_df = dim_customer.alias("customer")
product_df = dim_product.alias("product")
date_df = dim_date.alias("date")
payment_df = dim_payment.alias("payment")

fact = orders_df.join(
    order_items_df,
    col("orders.order_id") == col("order_items.order_id"),
    "inner"
)

fact = fact.join(
    customer_df,
    col("orders.customer_id") == col("customer.customer_id"),
    "inner"
)

fact = fact.join(
    product_df,
    col("order_items.product_id") == col("product.product_id"),
    "inner"
)

fact = fact.join(
    date_df,
    to_date(col("orders.order_date")) == col("date.full_date"),
    "inner"
)

fact = fact.join(
    payment_df,
    col("orders.order_id") == col("payment.order_id"),
    "inner"
)

fact = fact.select(
    col("orders.order_id").alias("order_id"),
    col("customer.customer_key").alias("customer_key"),
    col("product.product_key").alias("product_key"),
    col("date.date_key").alias("date_key"),
    col("payment.payment_key").alias("payment_key"),
    col("order_items.quantity").alias("quantity"),
    col("order_items.unit_price").alias("unit_price"),
    col("order_items.discount").alias("discount"),
    col("order_items.line_total").alias("revenue")
)



fact.printSchema()

fact.show(10, truncate=False)

print("Fact rows:", fact.count())

output_path = "data/warehouse/fact_orders"

(
    fact.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Fact orders created successfully.")

spark.stop()