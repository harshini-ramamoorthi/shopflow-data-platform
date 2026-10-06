SELECT
    date_key,
    full_date,
    year,
    month,
    month_name,
    quarter
FROM read_parquet('{{ var("shopflow_data_path") }}/warehouse/dim_date/*.parquet')
