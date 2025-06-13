{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals calculation
-- Replaces BigQuery UNNEST with Snowflake FLATTEN for array processing
SELECT
    customer_id,
    SUM(item.value:quantity::NUMBER * item.value:price::NUMBER) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('int_sales_with_items') }} sales,
     LATERAL FLATTEN(input => sales.items) AS item
GROUP BY
    customer_id