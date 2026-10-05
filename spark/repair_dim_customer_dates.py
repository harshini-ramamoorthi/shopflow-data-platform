from pathlib import Path
import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, when, to_timestamp, count
)

DIM_PATH = "data/warehouse/dim_customer"
ORDERS_PATH = "data/staging/orders"

spark = SparkSession.builder.appName("RepairCustomerEffectiveDates").getOrCreate()

dim_path = Path(DIM_PATH)
temp_path = Path(DIM_PATH + "_repair_temp")
backup_path = Path(
    DIM_PATH + "_backup_" + datetime.now().strftime("%Y%m%d_%H%M%S")
)

if not dim_path.exists():
    raise RuntimeError(f"Dimension not found: {DIM_PATH}")

if temp_path.exists() or backup_path.exists():
    raise RuntimeError("Temporary or backup path already exists. Stopping safely.")

dim = spark.read.parquet(DIM_PATH)
orders = spark.read.parquet(ORDERS_PATH)

required_dim = {
    "customer_id", "customer_key", "signup_date",
    "effective_from", "effective_to"
}
required_orders = {"order_id", "customer_id", "order_date"}

if not required_dim.issubset(set(dim.columns)):
    raise RuntimeError(f"Missing dimension columns: {required_dim - set(dim.columns)}")

if not required_orders.issubset(set(orders.columns)):
    raise RuntimeError(f"Missing order columns: {required_orders - set(orders.columns)}")

# Update only the earliest version for each customer.
window = Window.partitionBy("customer_id").orderBy(
    col("effective_from").asc(),
    col("customer_key").asc()
)

candidate = (
    dim.withColumn("_version_number", row_number().over(window))
       .withColumn(
           "effective_from",
           when(
               col("_version_number") == 1,
               col("signup_date").cast("timestamp")
           ).otherwise(col("effective_from"))
       )
       .drop("_version_number")
)

if candidate.filter(col("effective_from").isNull()).count() > 0:
    raise RuntimeError("Null effective_from found. No changes made.")

if candidate.count() != dim.count():
    raise RuntimeError("Dimension row count changed unexpectedly. No changes made.")

def count_bad_order_matches(dimension):
    order_rows = (
        orders.select(
            "order_id",
            "customer_id",
            to_timestamp("order_date").alias("order_ts")
        ).dropDuplicates(["order_id"])
    )

    versions = dimension.select(
        "customer_id", "customer_key", "effective_from", "effective_to"
    )

    matches = (
        order_rows.alias("o")
        .join(
            versions.alias("d"),
            (col("o.customer_id") == col("d.customer_id"))
            & (col("o.order_ts") >= col("d.effective_from"))
            & (
                col("d.effective_to").isNull()
                | (col("o.order_ts") < col("d.effective_to"))
            ),
            "left"
        )
        .groupBy(col("o.order_id").alias("order_id"))
        .agg(count(col("d.customer_key")).alias("match_count"))
    )

    return matches.filter(col("match_count") != 1).count(), matches.count()

bad_before, total_orders = count_bad_order_matches(candidate)
print(f"Orders checked: {total_orders}")
print(f"Orders without exactly one matching version: {bad_before}")

if total_orders != 300 or bad_before != 0:
    raise RuntimeError("Validation failed. Dimension was NOT changed.")

# Write candidate separately, then preserve the original before replacement.
candidate.write.mode("error").parquet(str(temp_path))
os.rename(dim_path, backup_path)

try:
    os.rename(temp_path, dim_path)
    repaired = spark.read.parquet(DIM_PATH)
    bad_after, _ = count_bad_order_matches(repaired)

    if bad_after != 0:
        raise RuntimeError("Post-write validation failed.")

    print(f"Repair successful. Original dimension backed up at: {backup_path}")

except Exception:
    if dim_path.exists():
        shutil.rmtree(dim_path)
    if backup_path.exists():
        os.rename(backup_path, dim_path)
    if temp_path.exists():
        shutil.rmtree(temp_path)
    raise

spark.stop()