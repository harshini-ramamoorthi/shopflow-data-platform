from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max
)
from pathlib import Path
import shutil

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# Start Spark
spark = SparkSession.builder \
    .appName("ApplyCustomerSCD2") \
    .master("local[*]") \
    .getOrCreate()


# File paths
incremental_path = "data/raw/incremental/customers.csv"
warehouse_path = "data/warehouse/dim_customer"
temp_path = "data/warehouse/dim_customer_scd2_temp"


# Read incremental customer changes
incremental_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(incremental_path)

incremental_count = incremental_df.count()

print("Incremental records:", incremental_count)

if incremental_count == 0:
    print("No customer changes found. Skipping SCD2 update.")
    spark.stop()
    raise SystemExit(0)


# Check for multiple records for the same customer
duplicate_customers = (
    incremental_df
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .select("customer_id")
    .collect()
)

if duplicate_customers:
    duplicate_ids = [row["customer_id"] for row in duplicate_customers]
    print("Multiple changes found for the same customer:", duplicate_ids)
    print("Please resolve duplicate customer records before running SCD2.")
    spark.stop()
    raise SystemExit(1)


# Read existing customer dimension
dim_customer = spark.read.parquet(warehouse_path)

print("Existing dimension records:", dim_customer.count())


# Find the latest customer key currently in the dimension
max_key = dim_customer.select(
    spark_max("customer_key")
).collect()[0][0]

print("Current maximum customer key:", max_key)


# Get the timestamp of the customer change
# Get all changed customer IDs
changed_ids = incremental_df.select(
    "customer_id"
).distinct()

# Get each customer's own change timestamp
updates = incremental_df.select(
    "customer_id",
    col("updated_at").alias("change_timestamp")
)

# Check that every changed customer has exactly one current version
current_counts = dim_customer \
    .filter(col("is_current") == True) \
    .groupBy("customer_id") \
    .count()

invalid_current = changed_ids.join(
    current_counts,
    "customer_id",
    "left"
).filter(
    col("count").isNull() | (col("count") != 1)
).collect()

if invalid_current:
    invalid_ids = [row["customer_id"] for row in invalid_current]
    print("Customers without exactly one current version:", invalid_ids)
    spark.stop()
    raise SystemExit(1)


# Check that each change timestamp is later than the current version's start
current_updates = dim_customer \
    .filter(col("is_current") == True) \
    .join(updates, "customer_id", "inner")

invalid_timestamps = current_updates.filter(
    col("change_timestamp").isNull() |
    col("effective_from").isNull() |
    (col("change_timestamp") <= col("effective_from"))
).select(
    "customer_id"
).collect()

if invalid_timestamps:
    invalid_ids = [row["customer_id"] for row in invalid_timestamps]
    print("Invalid change timestamps for customers:", invalid_ids)
    spark.stop()
    raise SystemExit(1)


# Expire the current version of each changed customer
expired_df = current_updates \
    .withColumn(
        "effective_to",
        col("change_timestamp")
    ) \
    .withColumn(
        "is_current",
        lit(False)
    ) \
    .drop("change_timestamp")


# Preserve customers that were not changed
unchanged_df = dim_customer.join(
    changed_ids,
    "customer_id",
    "left_anti"
)


# Preserve older historical versions of changed customers
historical_df = dim_customer \
    .filter(col("is_current") == False) \
    .join(
        changed_ids,
        "customer_id",
        "inner"
    )


# Assign a unique customer key to every new version
key_window = Window.orderBy("customer_id")

new_customer = incremental_df \
    .withColumn(
        "customer_key",
        lit(max_key if max_key is not None else 0) +
        row_number().over(key_window)
    ) \
    .withColumn(
        "effective_from",
        col("updated_at").cast("timestamp")
    ) \
    .withColumn(
        "effective_to",
        lit(None).cast("timestamp")
    ) \
    .withColumn(
        "is_current",
        lit(True)
    )


# Select columns in the same order as the dimension
new_customer = new_customer.select(
    "customer_key",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "country",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
)


# Combine all dimension records
final_dim_customer = unchanged_df \
    .unionByName(historical_df) \
    .unionByName(expired_df) \
    .unionByName(new_customer)


print("Final dimension records:", final_dim_customer.count())


# Write to temporary location first
final_dim_customer.write \
    .mode("overwrite") \
    .parquet(temp_path)

print("Temporary SCD2 write successful.")


# Replace the warehouse only after successful write
# Replace the warehouse while keeping a recoverable backup
warehouse = Path(warehouse_path)
temp = Path(temp_path)
backup = Path("data/warehouse/dim_customer_scd2_backup")

if backup.exists():
    print("Backup already exists. Stop and inspect it before continuing.")
    spark.stop()
    raise SystemExit(1)

if not temp.exists():
    print("Temporary output was not found. Existing warehouse was not changed.")
    spark.stop()
    raise SystemExit(1)

if warehouse.exists():
    warehouse.rename(backup)

try:
    temp.rename(warehouse)
except Exception:
    # Restore the original warehouse if the replacement fails
    if backup.exists() and not warehouse.exists():
        backup.rename(warehouse)
    raise

print("SCD2 output installed successfully.")
print("Previous dimension retained at:", backup)
print("SCD Type 2 customer update completed successfully!")


# Final verification
final_df = spark.read.parquet(warehouse_path)

print("Final warehouse records:", final_df.count())

print("Updated customer history:")
final_df.join(
    changed_ids,
    "customer_id",
    "inner"
).orderBy(
    "customer_id",
    "effective_from"
).show(truncate=False)

spark.stop()