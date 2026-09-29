SELECT
    product_id - 1 AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    supplier,
    updated_at
FROM {{ ref('stg_products') }}