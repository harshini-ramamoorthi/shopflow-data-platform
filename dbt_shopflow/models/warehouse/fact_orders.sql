SELECT
    order_id,
    customer_key,
    product_key,
    date_key,
    payment_key,
    quantity,
    unit_price,
    discount,
    revenue
FROM read_parquet('../data/warehouse/fact_orders/*.parquet')