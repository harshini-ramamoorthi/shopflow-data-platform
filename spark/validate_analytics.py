from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, countDistinct


spark = (
    SparkSession.builder
    .appName("ShopFlow Analytics Validation")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# Load datasets
# --------------------------------------------------

fact = spark.read.parquet(
    "data/warehouse/fact_orders"
)

dim_product = spark.read.parquet(
    "data/warehouse/dim_product"
)

dim_customer = spark.read.parquet(
    "data/warehouse/dim_customer"
)

payments = spark.read.parquet(
    "data/staging/payments"
)

daily_sales = spark.read.parquet(
    "data/analytics/daily_sales"
)

product_performance = spark.read.parquet(
    "data/analytics/product_performance"
)

customer_sales = spark.read.parquet(
    "data/analytics/customer_sales"
)

category_performance = spark.read.parquet(
    "data/analytics/category_performance"
)

payment_analysis = spark.read.parquet(
    "data/analytics/payment_analysis"
)


print("\n========== SHOPFLOW ANALYTICS VALIDATION ==========\n")


# --------------------------------------------------
# 1. Daily Sales Validation
# --------------------------------------------------

print("1. DAILY SALES VALIDATION")

fact_revenue = fact.select(
    sum("revenue").alias("total")
).collect()[0]["total"]

daily_revenue = daily_sales.select(
    sum("total_revenue").alias("total")
).collect()[0]["total"]

print("Fact revenue:", fact_revenue)
print("Daily sales revenue:", daily_revenue)

if fact_revenue == daily_revenue:
    print("PASS: Daily sales revenue matches fact revenue.")
else:
    print("FAIL: Daily sales revenue does not match fact revenue.")


fact_orders = fact.select(
    countDistinct("order_id").alias("total")
).collect()[0]["total"]

daily_orders = daily_sales.select(
    sum("total_orders").alias("total")
).collect()[0]["total"]

print("Fact orders:", fact_orders)
print("Daily sales orders:", daily_orders)

if fact_orders == daily_orders:
    print("PASS: Daily sales order count matches fact orders.")
else:
    print("FAIL: Daily sales order count does not match fact orders.")


# --------------------------------------------------
# 2. Product Performance Validation
# --------------------------------------------------

print("\n2. PRODUCT PERFORMANCE VALIDATION")

product_count = dim_product.count()
analytics_product_count = product_performance.count()

print("Products in dimension:", product_count)
print("Products in analytics:", analytics_product_count)

if product_count == analytics_product_count:
    print("PASS: Product analytics covers all products.")
else:
    print("INFO: Some products have no sales.")


product_revenue = product_performance.select(
    sum("total_revenue").alias("total")
).collect()[0]["total"]

print("Fact revenue:", fact_revenue)
print("Product analytics revenue:", product_revenue)

if fact_revenue == product_revenue:
    print("PASS: Product revenue matches fact revenue.")
else:
    print("FAIL: Product revenue does not match fact revenue.")


# --------------------------------------------------
# 3. Customer Sales Validation
# --------------------------------------------------

print("\n3. CUSTOMER SALES VALIDATION")

customers_with_sales = fact.select(
    "customer_key"
).distinct().count()

customer_analytics_count = customer_sales.count()

print("Customers with sales:", customers_with_sales)
print("Customers in analytics:", customer_analytics_count)

if customers_with_sales == customer_analytics_count:
    print("PASS: Customer analytics covers all customers with sales.")
else:
    print("FAIL: Customer analytics customer count mismatch.")


customer_revenue = customer_sales.select(
    sum("total_revenue").alias("total")
).collect()[0]["total"]

print("Fact revenue:", fact_revenue)
print("Customer analytics revenue:", customer_revenue)

if fact_revenue == customer_revenue:
    print("PASS: Customer revenue matches fact revenue.")
else:
    print("FAIL: Customer revenue does not match fact revenue.")


# --------------------------------------------------
# 4. Category Performance Validation
# --------------------------------------------------

print("\n4. CATEGORY PERFORMANCE VALIDATION")

category_count = dim_product.select(
    "category"
).distinct().count()

analytics_category_count = category_performance.count()

print("Categories in product dimension:", category_count)
print("Categories in analytics:", analytics_category_count)

if category_count == analytics_category_count:
    print("PASS: Category analytics covers all categories.")
else:
    print("FAIL: Category analytics category count mismatch.")


category_revenue = category_performance.select(
    sum("total_revenue").alias("total")
).collect()[0]["total"]

print("Fact revenue:", fact_revenue)
print("Category analytics revenue:", category_revenue)

if fact_revenue == category_revenue:
    print("PASS: Category revenue matches fact revenue.")
else:
    print("FAIL: Category revenue does not match fact revenue.")


# --------------------------------------------------
# 5. Payment Analysis Validation
# --------------------------------------------------

print("\n5. PAYMENT ANALYSIS VALIDATION")

source_payment_count = payments.count()

analytics_payment_count = payment_analysis.select(
    sum("payment_count").alias("total")
).collect()[0]["total"]

print("Source payments:", source_payment_count)
print("Analytics payments:", analytics_payment_count)

if source_payment_count == analytics_payment_count:
    print("PASS: Payment count matches source payments.")
else:
    print("FAIL: Payment count does not match source payments.")


source_payment_amount = payments.select(
    sum("amount").alias("total")
).collect()[0]["total"]

analytics_payment_amount = payment_analysis.select(
    sum("total_payment_amount").alias("total")
).collect()[0]["total"]

print("Source payment amount:", source_payment_amount)
print("Analytics payment amount:", analytics_payment_amount)

if source_payment_amount == analytics_payment_amount:
    print("PASS: Payment amount matches source payments.")
else:
    print("FAIL: Payment amount does not match source payments.")


# --------------------------------------------------
# 6. NULL Validation
# --------------------------------------------------

print("\n6. NULL VALIDATION")

null_daily = daily_sales.filter(
    col("total_revenue").isNull()
).count()

null_product = product_performance.filter(
    col("total_revenue").isNull()
).count()

null_customer = customer_sales.filter(
    col("total_revenue").isNull()
).count()

null_category = category_performance.filter(
    col("total_revenue").isNull()
).count()

null_payment = payment_analysis.filter(
    col("total_payment_amount").isNull()
).count()

print("Daily sales NULL revenue:", null_daily)
print("Product NULL revenue:", null_product)
print("Customer NULL revenue:", null_customer)
print("Category NULL revenue:", null_category)
print("Payment NULL amount:", null_payment)

if (
    null_daily == 0
    and null_product == 0
    and null_customer == 0
    and null_category == 0
    and null_payment == 0
):
    print("PASS: No NULL analytical measures found.")
else:
    print("FAIL: NULL analytical measures found.")


# --------------------------------------------------
# Final
# --------------------------------------------------

print("\n========== VALIDATION COMPLETED ==========\n")

spark.stop()