CREATE TABLE pipeline_metadata (
    pipeline_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    last_processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (pipeline_name, table_name)
);