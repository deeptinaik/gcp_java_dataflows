/*
  Intermediate model: Ranked orders
  Calculates order-level totals and rankings
  Replaces the 'ranked_orders' CTE from original BigQuery query
*/

{{ config(
    materialized='ephemeral'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    SUM(quantity * price) AS order_total,
    COUNT(*) AS items_in_order,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_sequence
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date