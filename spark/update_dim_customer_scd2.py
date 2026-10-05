from functools import reduce
from pathlib import Path
import sys
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DIMENSION_PATH = "data/warehouse/dim_customer"
STAGING_PATH = "data/staging/customers"
CANDIDATE_PATH = "data/warehouse/dim_customer_scd2_candidate"

TRACKED_COLUMNS = [
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "country",
]

parser = argparse.ArgumentParser()
parser.add_argument("--dimension", default=DIMENSION_PATH)
parser.add_argument("--staging", default=STAGING_PATH)
parser.add_argument("--candidate", default=CANDIDATE_PATH)
parser.add_argument("--write-candidate", action="store_true")
args = parser.parse_args()

DIMENSION_PATH = args.dimension
STAGING_PATH = args.staging
CANDIDATE_PATH = args.candidate


spark = (
    SparkSession.builder
    .appName("ShopFlow Incremental Customer SCD2")
    .master("local[*]")
    .getOrCreate()
)

try:
    # Read the existing dimension and incoming customer data.
    dimension = spark.read.parquet(DIMENSION_PATH)
    source = spark.read.parquet(STAGING_PATH)

    # Basic input checks.
    required_dimension_columns = [
        "customer_key",
        "customer_id",
        *TRACKED_COLUMNS,
        "signup_date",
        "effective_from",
        "effective_to",
        "is_current",
    ]

    required_source_columns = [
        "customer_id",
        *TRACKED_COLUMNS,
        "signup_date",
        "updated_at",
    ]

    for name in required_dimension_columns:
        if name not in dimension.columns:
            raise ValueError(f"Missing dimension column: {name}")

    for name in required_source_columns:
        if name not in source.columns:
            raise ValueError(f"Missing staging column: {name}")

    # Each customer must have at most one current dimension row.
    duplicate_current = (
        dimension
        .filter(F.col("is_current") == True)
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_current > 0:
        raise ValueError("A customer has more than one current dimension row.")

    # The source must contain only one row per customer.
    duplicate_source = (
        source.groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_source > 0:
        raise ValueError("Staging contains duplicate customer IDs.")

    current = dimension.filter(F.col("is_current") == True)

    source_alias = source.alias("s")
    current_alias = current.alias("d")

    joined = source_alias.join(
        current_alias,
        F.col("s.customer_id") == F.col("d.customer_id"),
        "left",
    )

    # Find existing customers whose tracked attributes changed.
    changed_conditions = [
        ~F.col(f"s.{name}").eqNullSafe(F.col(f"d.{name}"))
        for name in TRACKED_COLUMNS
    ]

    attributes_changed = reduce(
        lambda left, right: left | right,
        changed_conditions,
    )

    changed = joined.filter(
        F.col("d.customer_key").isNotNull() & attributes_changed
    )

    # Find customers that are new to the dimension.
    new_customers = joined.filter(F.col("d.customer_key").isNull())

    # A changed record needs a valid timestamp after its current version began.
    invalid_change_time = changed.filter(
        F.col("s.updated_at").isNull()
        | (F.col("s.updated_at") <= F.col("d.effective_from"))
    ).count()

    if invalid_change_time > 0:
        raise ValueError(
            "A changed customer has a missing or invalid updated_at timestamp."
        )

    changed_count = changed.count()
    new_count = new_customers.count()

    print("Current dimension rows:", dimension.count())
    print("Changed customers:", changed_count)
    print("New customers:", new_count)

    
    # Close the previous version for each changed customer.
    changed_times = changed.select(
        F.col("s.customer_id").alias("customer_id"),
        F.col("s.updated_at").alias("change_time"),
    )

    dimension_with_changes = dimension.alias("d").join(
        changed_times.alias("t"),
        F.col("d.customer_id") == F.col("t.customer_id"),
        "left",
    )

    closed_columns = []

    for name in dimension.columns:
        if name == "effective_to":
            expression = F.when(
                (F.col("d.is_current") == True)
                & F.col("t.change_time").isNotNull(),
                F.col("t.change_time"),
            ).otherwise(F.col("d.effective_to")).alias(name)

        elif name == "is_current":
            expression = F.when(
                (F.col("d.is_current") == True)
                & F.col("t.change_time").isNotNull(),
                F.lit(False),
            ).otherwise(F.col("d.is_current")).alias(name)

        else:
            expression = F.col(f"d.{name}").alias(name)

        closed_columns.append(expression)

    closed_dimension = dimension_with_changes.select(*closed_columns)
    
    # Assign new, unique keys after the current maximum key.
    max_key = dimension.agg(
        F.max("customer_key").alias("max_key")
    ).first()["max_key"]

    if max_key is None:
        max_key = -1

    next_key = max_key + 1

    # Changed customers get a new version beginning at updated_at.
    changed_new_rows = changed.select(
        F.col("s.customer_id").alias("customer_id"),
        *[F.col(f"s.{name}").alias(name) for name in TRACKED_COLUMNS],
        F.col("s.signup_date").alias("signup_date"),
        F.col("s.updated_at").alias("effective_from"),
    )

    # New customers start at their signup date.
    new_customer_rows = new_customers.select(
        F.col("s.customer_id").alias("customer_id"),
        *[F.col(f"s.{name}").alias(name) for name in TRACKED_COLUMNS],
        F.col("s.signup_date").alias("signup_date"),
        F.col("s.signup_date").cast("timestamp").alias("effective_from"),
    )

    additions = changed_new_rows.unionByName(new_customer_rows)

    key_window = Window.orderBy("customer_id")

    additions = (
        additions
        .withColumn(
            "customer_key",
            (F.row_number().over(key_window) + F.lit(next_key - 1)).cast("long"),
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast("timestamp"),
        )
        .withColumn("is_current", F.lit(True))
        .select(*dimension.columns)
    )

    candidate = closed_dimension.unionByName(additions)

    # Validate the candidate before writing anything.
    duplicate_keys = (
        candidate.groupBy("customer_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    multiple_current = (
        candidate.filter(F.col("is_current") == True)
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    invalid_ranges = candidate.filter(
        F.col("effective_to").isNotNull()
        & (F.col("effective_to") <= F.col("effective_from"))
    ).count()

    if duplicate_keys > 0:
        raise ValueError("Candidate contains duplicate customer keys.")

    if multiple_current > 0:
        raise ValueError("Candidate contains multiple current versions.")

    if invalid_ranges > 0:
        raise ValueError("Candidate contains invalid effective date ranges.")

    print("Candidate dimension rows:", candidate.count())
    print("Candidate validation: PASSED")

    print("Customer history preview:")
    candidate.orderBy("customer_id", "effective_from").show(
        20,
        truncate=False,
    )

    # Default behavior is dry-run. Writing requires an explicit argument.
    if args.write_candidate:
        if Path(CANDIDATE_PATH).exists():
            raise FileExistsError(
                f"{CANDIDATE_PATH} already exists. "
                "Inspect it before deciding whether to remove it."
            )

        candidate.write.mode("error").parquet(CANDIDATE_PATH)

        print("Candidate written to:", CANDIDATE_PATH)
        print("The real dimension and fact table were not modified.")
    else:
        print("DRY RUN: no files were written.")
        print("To write the candidate, run with --write-candidate.")

finally:
    spark.stop()