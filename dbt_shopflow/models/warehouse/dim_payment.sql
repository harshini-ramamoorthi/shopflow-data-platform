SELECT
    payment_key,
    payment_id,
    order_id,
    payment_method,
    payment_status
FROM read_parquet('../data/warehouse/dim_payment/*.parquet')
