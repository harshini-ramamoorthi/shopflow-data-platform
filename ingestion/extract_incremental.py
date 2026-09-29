import pandas as pd
from sqlalchemy import text
from ingestion.db_config import get_sqlalchemy_engine
from pathlib import Path

engine = get_sqlalchemy_engine()


def extract_incremental_customers():

    metadata_query = text("""
        SELECT last_processed_at
        FROM pipeline_metadata
        WHERE pipeline_name = 'shopflow'
          AND table_name = 'customers'
    """)

    with engine.connect() as connection:
        result = connection.execute(metadata_query).fetchone()

    last_processed_at = result[0]

    print("Last processed timestamp:", last_processed_at)

    query = text("""
        SELECT *
        FROM customers
        WHERE updated_at > :last_processed_at
        ORDER BY updated_at
    """)

    with engine.connect() as connection:
        df = pd.read_sql_query(
            query,
            connection,
            params={
                "last_processed_at": last_processed_at
            }
        )

    output_directory = Path("data/raw/incremental")
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / "customers.csv"

    df.to_csv(
        output_file,
        index=False
    )

    print("Incremental customers extracted:", len(df))
    print("Output file:", output_file)

    return df


if __name__ == "__main__":
    extract_incremental_customers()
    engine.dispose()