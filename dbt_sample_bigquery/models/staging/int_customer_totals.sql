{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model for customer total calculations
SELECT
    customer_id,
    SUM((items.value:quantity)::NUMBER * (items.value:price)::NUMBER) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('stg_orders') }},
     LATERAL FLATTEN(input => items) AS items
GROUP BY
    customer_id