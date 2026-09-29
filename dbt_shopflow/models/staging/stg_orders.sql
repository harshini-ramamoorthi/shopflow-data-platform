SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    shipping_city,
    shipping_state,
    updated_at
FROM read_parquet('data/staging/orders/*.parquet')
