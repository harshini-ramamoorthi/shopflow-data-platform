SELECT *
FROM {{ ref('fact_orders') }}
WHERE ABS(revenue - (quantity * unit_price - discount)) > 0.01
