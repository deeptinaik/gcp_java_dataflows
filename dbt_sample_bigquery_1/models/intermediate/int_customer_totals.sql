{{
  config(
    materialized='ephemeral'
  )
}}

-- Intermediate model: Customer totals
-- Calculates customer-level spending totals (similar to original customer_totals CTE)
SELECT
    customer_id,
    SUM(order_total) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_line_items,
    AVG(order_total) AS avg_order_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM {{ ref('int_sales_aggregated') }}
GROUP BY customer_id