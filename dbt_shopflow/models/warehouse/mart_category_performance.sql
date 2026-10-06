{{ config(materialized='table') }}

SELECT
    p.category,

    SUM(f.quantity) AS units_sold,

    ROUND(SUM(f.revenue), 2) AS total_revenue

FROM {{ ref('fact_orders') }} AS f

INNER JOIN {{ ref('dim_product') }} AS p
    ON f.product_key = p.product_key

GROUP BY
    p.category
