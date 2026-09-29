from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from pathlib import Path
import shutil


spark = SparkSession.builder \
    .appName("IncrementalCustomerLoad") \
    .master("local[*]") \
    .getOrCreate()


source_file = "data/raw/incremental/customers.csv"
warehouse_path = "data/warehouse/dim_customer"
temp_path = "data/warehouse/dim_customer_temp"


# Read incremental customer changes
incremental_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(source_file)

print("Incremental records:", incremental_df.count())


# Read existing warehouse dimension
dim_customer = spark.read.parquet(warehouse_path)


print("Existing warehouse records:", dim_customer.count())


# Join existing dimension with incremental changes
joined_df = dim_customer.alias("dim").join(
    incremental_df.alias("inc"),
    col("dim.customer_id") == col("inc.customer_id"),
    "left"
)


# Keep existing values when there is no incremental change.
# Use incremental values when a customer was changed.
final_dim_customer = joined_df.select(
    col("dim.customer_key"),
    col("dim.customer_id"),

    coalesce(
        col("inc.first_name"),
        col("dim.first_name")
    ).alias("first_name"),

    coalesce(
        col("inc.last_name"),
        col("dim.last_name")
    ).alias("last_name"),

    coalesce(
        col("inc.email"),
        col("dim.email")
    ).alias("email"),

    coalesce(
        col("inc.city"),
        col("dim.city")
    ).alias("city"),

    coalesce(
        col("inc.state"),
        col("dim.state")
    ).alias("state"),

    coalesce(
        col("inc.country"),
        col("dim.country")
    ).alias("country"),

    coalesce(
        col("inc.signup_date"),
        col("dim.signup_date")
    ).alias("signup_date")
)


print("Final warehouse records:", final_dim_customer.count())


# Write to temporary location first
final_dim_customer.write \
    .mode("overwrite") \
    .parquet(temp_path)


# Replace the old warehouse directory
warehouse = Path(warehouse_path)
temp = Path(temp_path)

if warehouse.exists():
    shutil.rmtree(warehouse)

shutil.move(temp, warehouse)


print("Incremental customer load completed successfully!")


spark.stop()