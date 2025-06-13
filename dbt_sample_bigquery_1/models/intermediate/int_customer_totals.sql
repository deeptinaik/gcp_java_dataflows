/*
  Intermediate model: Customer totals
  Calculates customer-level aggregations
  Replaces the 'customer_totals' CTE from original BigQuery query
*/

{{ config(
    materialized='ephemeral'
) }}

SELECT
    customer_id,
    SUM(quantity * price) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items_purchased,
    AVG(quantity * price) AS avg_item_value,
    MAX(order_date) AS last_order_date,
    MIN(order_date) AS first_order_date
FROM {{ ref('stg_orders') }}
GROUP BY
    customer_id