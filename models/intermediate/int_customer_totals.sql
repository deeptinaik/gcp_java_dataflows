{{ config(materialized='ephemeral') }}

-- Intermediate model: Customer totals
-- Replicates the 'customer_totals' CTE from the original BigQuery
-- Calculates total spent and order count per customer

SELECT
    customer_id,
    SUM(quantity * price) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM 
    {{ ref('int_sales_aggregated') }}, 
    UNNEST(items)
GROUP BY
    customer_id