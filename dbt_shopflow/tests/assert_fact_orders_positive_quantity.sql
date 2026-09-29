SELECT *
FROM {{ ref('fact_orders') }}
WHERE quantity <= 0
