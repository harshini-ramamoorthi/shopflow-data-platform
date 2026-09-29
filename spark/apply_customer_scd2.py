from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max
)
from pathlib import Path
import shutil


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

print("Incremental records:", incremental_df.count())


# Read existing customer dimension
dim_customer = spark.read.parquet(warehouse_path)

print("Existing dimension records:", dim_customer.count())


# Find the latest customer key currently in the dimension
max_key = dim_customer.select(
    spark_max("customer_key")
).collect()[0][0]

print("Current maximum customer key:", max_key)


# Get the timestamp of the customer change
change_timestamp = incremental_df.select(
    spark_max("updated_at")
).collect()[0][0]

print("Change timestamp:", change_timestamp)


# Identify the changed customer
changed_customer_id = incremental_df.select(
    "customer_id"
).collect()[0][0]

print("Changed customer ID:", changed_customer_id)


# Expire the existing current version
expired_df = dim_customer \
    .filter(col("customer_id") == changed_customer_id) \
    .withColumn(
        "effective_to",
        lit(change_timestamp)
    ) \
    .withColumn(
        "is_current",
        lit(False)
    )


# Keep all customers that were not changed
unchanged_df = dim_customer.filter(
    col("customer_id") != changed_customer_id
)


# Read the new customer version
new_customer = incremental_df \
    .withColumn(
        "customer_key",
        lit(max_key + 1)
    ) \
    .withColumn(
        "effective_from",
        col("updated_at")
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


# Combine all records
final_dim_customer = unchanged_df \
    .unionByName(expired_df) \
    .unionByName(new_customer)


print("Final dimension records:", final_dim_customer.count())


# Write to temporary location first
final_dim_customer.write \
    .mode("overwrite") \
    .parquet(temp_path)

print("Temporary SCD2 write successful.")


# Replace the warehouse only after successful write
warehouse = Path(warehouse_path)
temp = Path(temp_path)

if warehouse.exists():
    shutil.rmtree(warehouse)

shutil.move(temp, warehouse)

print("SCD Type 2 customer update completed successfully!")


# Final verification
final_df = spark.read.parquet(warehouse_path)

print("Final warehouse records:", final_df.count())

print("Customer history:")
final_df.filter(
    col("customer_id") == changed_customer_id
).orderBy(
    "effective_from"
).show(truncate=False)


spark.stop()