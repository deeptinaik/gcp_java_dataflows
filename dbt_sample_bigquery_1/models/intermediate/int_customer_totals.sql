{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals
-- Replicates the 'customer_totals' CTE from original BigQuery SQL
-- Calculates total spent and order count per customer

SELECT
    customer_id,
    SUM(order_total) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    AVG(order_total) AS avg_order_value
FROM {{ ref('int_sales_aggregated') }}
GROUP BY customer_id