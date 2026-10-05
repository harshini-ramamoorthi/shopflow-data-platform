SELECT
    date_key,
    full_date,
    year,
    month,
    month_name,
    quarter
FROM read_parquet('../data/warehouse/dim_date/*.parquet')
