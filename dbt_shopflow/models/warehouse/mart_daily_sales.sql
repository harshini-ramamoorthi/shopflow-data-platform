{{ config(materialized='table') }}

SELECT
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter,

    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_sold,
    SUM(f.revenue) AS total_revenue,

    SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0)
        AS average_order_value

FROM {{ ref('fact_orders') }} AS f

INNER JOIN {{ ref('dim_date') }} AS d
    ON f.date_key = d.date_key

GROUP BY
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter
