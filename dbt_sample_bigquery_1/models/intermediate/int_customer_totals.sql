{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals
-- Equivalent to the 'customer_totals' CTE in the original BigQuery
-- Uses flattened data approach for Snowflake compatibility

SELECT
    customer_id,
    SUM(quantity * price) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(product_id) AS total_items,
    AVG(quantity * price) AS avg_line_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM {{ ref('stg_orders') }}
GROUP BY
    customer_id