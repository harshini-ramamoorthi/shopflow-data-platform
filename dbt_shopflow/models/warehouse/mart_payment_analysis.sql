{{ config(materialized='table') }}

SELECT
    payment_method,
    payment_status,

    COUNT(payment_id) AS payment_count,

    ROUND(SUM(amount), 2) AS total_payment_amount

FROM {{ ref('stg_payments') }}

GROUP BY
    payment_method,
    payment_status
