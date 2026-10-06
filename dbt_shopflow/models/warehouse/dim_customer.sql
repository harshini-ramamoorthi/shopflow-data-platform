SELECT
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    signup_date,
    effective_from,
    effective_to,
    is_current
FROM read_parquet('{{ var("shopflow_data_path") }}/warehouse/dim_customer/*.parquet')
