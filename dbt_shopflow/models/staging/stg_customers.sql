SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    signup_date,
    updated_at
FROM read_parquet('{{ var("shopflow_data_path") }}/staging/customers/*.parquet')
