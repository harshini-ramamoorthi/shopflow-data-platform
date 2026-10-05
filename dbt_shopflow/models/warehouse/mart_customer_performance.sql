{{ config(materialized='table') }}

WITH customer_sales AS (
    SELECT
        f.customer_key,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_items_purchased,
        SUM(f.revenue) AS total_revenue,
        MIN(d.full_date) AS first_purchase_date,
        MAX(d.full_date) AS most_recent_purchase_date

    FROM {{ ref('fact_orders') }} AS f

    INNER JOIN {{ ref('dim_date') }} AS d
        ON f.date_key = d.date_key

    GROUP BY f.customer_key
)

SELECT
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state,
    c.country,
    c.is_current,

    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(s.total_items_purchased, 0) AS total_items_purchased,
    COALESCE(s.total_revenue, 0) AS total_revenue,
    s.first_purchase_date,
    s.most_recent_purchase_date

FROM {{ ref('dim_customer') }} AS c

LEFT JOIN customer_sales AS s
    ON c.customer_key = s.customer_key
