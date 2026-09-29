from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum
)

spark = (
    SparkSession.builder
    .appName("ShopFlow Daily Sales Analytics")
    .master("local[*]")
    .getOrCreate()
)

fact = spark.read.parquet("data/warehouse/fact_orders")
dim_date = spark.read.parquet("data/warehouse/dim_date")

daily_sales = fact.join(
    dim_date,
    fact.date_key == dim_date.date_key,
    "inner"
)

daily_sales = daily_sales.groupBy(
    "full_date",
    "year",
    "month",
    "month_name",
    "quarter"
).agg(
    countDistinct("order_id").alias("total_orders"),
    sum("quantity").alias("units_sold"),
    sum("revenue").alias("total_revenue")
)

daily_sales = daily_sales.orderBy("full_date")

daily_sales.printSchema()

daily_sales.show(10, truncate=False)

print("Daily sales rows:", daily_sales.count())

output_path = "data/analytics/daily_sales"

(
    daily_sales.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Daily sales analytics created successfully.")

spark.stop()