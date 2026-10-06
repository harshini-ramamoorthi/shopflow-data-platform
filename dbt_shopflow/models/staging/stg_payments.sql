SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_date
-- FROM read_parquet('../data/staging/payments/*.parquet')

FROM read_parquet('{{ var("shopflow_data_path") }}/staging/payments/*.parquet')
