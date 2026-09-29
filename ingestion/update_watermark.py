from sqlalchemy import text
from ingestion.db_config import get_sqlalchemy_engine

engine = get_sqlalchemy_engine()


def update_watermark(table_name, new_timestamp):

    query = text("""
        UPDATE pipeline_metadata
        SET last_processed_at = :new_timestamp,
            updated_at = CURRENT_TIMESTAMP
        WHERE pipeline_name = 'shopflow'
          AND table_name = :table_name
    """)

    with engine.begin() as connection:
        connection.execute(
            query,
            {
                "new_timestamp": new_timestamp,
                "table_name": table_name
            }
        )

    print("Watermark updated successfully!")
    print("Table:", table_name)
    print("New watermark:", new_timestamp)