{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model for ranking orders by customer
SELECT
    order_id,
    customer_id,
    order_date,
    SUM((items.value:quantity)::NUMBER * (items.value:price)::NUMBER) AS order_total,
    {{ rank_orders_by_date('customer_id', 'order_date') }} AS order_rank
FROM {{ ref('stg_orders') }},
     LATERAL FLATTEN(input => items) AS items
GROUP BY
    order_id, 
    customer_id, 
    order_date