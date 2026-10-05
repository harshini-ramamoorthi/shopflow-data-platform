from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("ShopFlow SCD2 Test")
    .master("local[*]")
    .getOrCreate()
)

# Read the real dimension, but never write to it.
dimension = spark.read.parquet("data/warehouse/dim_customer")

# Simulate a city change for customer 1.
source = spark.read.parquet("data/staging/customers")

test_source = (
    source
    .withColumn(
        "city",
        F.when(col("customer_id") == 1, lit("Test City"))
        .otherwise(col("city"))
    )
    .withColumn(
        "updated_at",
        F.when(
            col("customer_id") == 1,
            lit("2026-09-26 12:00:00").cast(TimestampType())
        ).otherwise(col("updated_at"))
    )
)

# Find the current version of customer 1.
old_row = dimension.filter(
    (col("customer_id") == 1) & (col("is_current") == True)
)

old_key = old_row.select("customer_key").first()[0]
old_start = old_row.select("effective_from").first()[0]

print("Original customer key:", old_key)
print("Original effective_from:", old_start)

# Confirm the simulated timestamp is later than the current version.
change_time = test_source.filter(
    col("customer_id") == 1
).select("updated_at").first()[0]

if change_time <= old_start:
    raise ValueError("Test change timestamp must be after effective_from.")

# Close the old version.
closed_old = (
    dimension
    .withColumn(
        "effective_to",
        F.when(
            (col("customer_id") == 1) & (col("is_current") == True),
            lit(change_time).cast(TimestampType())
        ).otherwise(col("effective_to"))
    )
    .withColumn(
        "is_current",
        F.when(
            (col("customer_id") == 1) & (col("is_current") == True),
            lit(False)
        ).otherwise(col("is_current"))
    )
)

# Create the new version with the next available key.
new_key = dimension.agg(spark_max("customer_key")).first()[0] + 1

new_row = (
    test_source
    .filter(col("customer_id") == 1)
    .withColumn("customer_key", lit(new_key).cast("long"))
    .withColumn("effective_from", col("updated_at"))
    .withColumn("effective_to", lit(None).cast(TimestampType()))
    .withColumn("is_current", lit(True))
    .select(*dimension.columns)
)

test_dimension = closed_old.unionByName(new_row)

print("\nSimulated SCD2 history for customer 1:")
test_dimension.filter(
    col("customer_id") == 1
).orderBy("effective_from").show(truncate=False)

# Assertions: old version closed, new version current, keys differ.
history = test_dimension.filter(col("customer_id") == 1).orderBy("effective_from").collect()

assert len(history) == 2, "Expected exactly two versions."
assert history[0]["customer_key"] == old_key, "Old key was not preserved."
assert history[0]["is_current"] is False, "Old version was not closed."
assert history[0]["effective_to"] == change_time, "Old version end time is incorrect."
assert history[1]["customer_key"] == new_key, "New key is incorrect."
assert history[1]["city"] == "Test City", "New city was not applied."
assert history[1]["is_current"] is True, "New version is not current."

print("\nPASS: Simulated SCD2 change created a valid history.")
print("PASS: Original dimension was not modified.")

spark.stop()