from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, round

spark = (
    SparkSession.builder
    .appName("ShopFlow Payment Analysis")
    .master("local[*]")
    .getOrCreate()
)

# Load payment staging data
payments = spark.read.parquet(
    "data/staging/payments"
)

# Aggregate payment information
payment_analysis = payments.groupBy(
    "payment_method",
    "payment_status"
).agg(
    count("payment_id").alias("payment_count"),
    round(sum("amount"), 2).alias("total_payment_amount")
)

# Sort results
payment_analysis = payment_analysis.orderBy(
    "payment_method",
    "payment_status"
)

# Inspect results
payment_analysis.printSchema()

payment_analysis.show(
    truncate=False
)

print(
    "Payment analysis rows:",
    payment_analysis.count()
)

# Save analytics output
output_path = "data/analytics/payment_analysis"

(
    payment_analysis.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Payment analysis analytics created successfully.")

spark.stop()