import pandas as pd
from pathlib import Path
from ingestion.db_config import get_sqlalchemy_engine

# -----------------------------
# Database connection
# -----------------------------

engine = get_sqlalchemy_engine()

# -----------------------------
# Raw data directory
# -----------------------------

raw_directory = Path("data/raw")
raw_directory.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Source tables
# -----------------------------

tables = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments"
]


# -----------------------------
# Extract tables
# -----------------------------

for table in tables:

    query = f"SELECT * FROM {table}"

    df = pd.read_sql_query(query, engine)

    output_file = raw_directory / f"{table}.csv"

    df.to_csv(output_file, index=False)

    print(f"{table}: {len(df)} rows extracted")


# -----------------------------
# Close connection
# -----------------------------

engine.dispose()

print("Raw data extraction completed.")