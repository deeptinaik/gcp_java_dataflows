{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals calculation
-- Replicates the 'customer_totals' CTE from original BigQuery
-- Since we can't easily UNNEST in Snowflake like BigQuery, we'll work from order items directly
SELECT
    customer_id,
    SUM(line_total) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items,
    -- Additional useful metrics
    AVG(line_total) AS avg_item_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM {{ ref('stg_orders') }}
GROUP BY
    customer_id