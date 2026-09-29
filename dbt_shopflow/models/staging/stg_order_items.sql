SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    line_total
FROM read_parquet('data/staging/order_items/*.parquet')
