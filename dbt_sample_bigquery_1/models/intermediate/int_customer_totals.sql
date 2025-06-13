{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals calculation
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN
SELECT
    customer_id,
    SUM(item_data.value:quantity::NUMBER * item_data.value:price::NUMBER) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('int_sales_with_items') }} sales,
     {{ unnest_array('sales', 'items') }} item_data
GROUP BY
    customer_id