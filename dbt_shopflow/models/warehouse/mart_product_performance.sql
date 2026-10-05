{{ config(materialized='table') }}

SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,

    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.quantity * f.unit_price) AS gross_sales,
    SUM(f.discount) AS total_discount,
    SUM(f.revenue) AS total_revenue

FROM {{ ref('fact_orders') }} AS f

INNER JOIN {{ ref('dim_product') }} AS p
    ON f.product_key = p.product_key

GROUP BY
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory
