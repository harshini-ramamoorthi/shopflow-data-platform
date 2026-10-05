from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ShopFlow Warehouse Validation")
    .master("local[*]")
    .getOrCreate()
)

fact = spark.read.parquet("data/warehouse/fact_orders")

# --------------------------------------------------
# Order Date Validation
# --------------------------------------------------

orders = spark.read.parquet("data/staging/orders")
customers = spark.read.parquet("data/staging/customers")

orders_with_customers = orders.join(
    customers.select("customer_id", "signup_date"),
    on="customer_id",
    how="inner"
)

invalid_order_dates = orders_with_customers.filter(
    col("order_date") < col("signup_date")
).count()

print("Orders before customer signup:", invalid_order_dates)

if invalid_order_dates == 0:
    print("PASS: All orders occur on or after customer signup.")
else:
    print("FAIL: Orders found before customer signup.")
    

fact_count = fact.count()

print("Fact rows:", fact_count)

if fact_count == 767:
    print("PASS: Fact row count is correct.")
else:
    print("FAIL: Unexpected fact row count.")

null_key_count = fact.filter(
    col("customer_key").isNull()
    | col("product_key").isNull()
    | col("date_key").isNull()
    | col("payment_key").isNull()
).count()

print("Rows with NULL dimension keys:", null_key_count)

if null_key_count == 0:
    print("PASS: No NULL dimension keys.")
else:
    print("FAIL: NULL dimension keys found.")

invalid_quantity_count = fact.filter(
    col("quantity") <= 0
).count()

print("Invalid quantity rows:", invalid_quantity_count)

if invalid_quantity_count == 0:
    print("PASS: All quantities are positive.")
else:
    print("FAIL: Invalid quantities found.")

invalid_revenue_count = fact.filter(
    col("revenue") != (
        (col("quantity") * col("unit_price"))
        - col("discount")
    )
).count()

print("Invalid revenue rows:", invalid_revenue_count)

if invalid_revenue_count == 0:
    print("PASS: Revenue calculations are correct.")
else:
    print("FAIL: Revenue calculation errors found.")

duplicate_count = (
    fact
    .groupBy(
        "order_id",
        "product_key",
        "quantity",
        "unit_price",
        "discount"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate fact groups:", duplicate_count)

if duplicate_count == 0:
    print("PASS: No duplicate fact groups found.")
else:
    print("FAIL: Duplicate fact groups found.")

print("\nWarehouse validation completed.")

spark.stop()