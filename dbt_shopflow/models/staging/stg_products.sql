SELECT
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    supplier,
    updated_at
FROM read_parquet('data/staging/products/*.parquet')
