{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders by customer
-- Equivalent to the 'ranked_orders' CTE in the original BigQuery

SELECT
    order_id,
    customer_id,
    order_date,
    SUM(quantity * price) AS order_total,
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_rank,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC, order_id
    ) AS order_sequence
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date