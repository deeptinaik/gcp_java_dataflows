{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals calculation
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN
SELECT
    customer_id,
    SUM(items_flattened.value:"quantity"::NUMBER * items_flattened.value:"price"::NUMBER) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('int_sales_with_items') }} sales,
    {{ unnest_array('sales.items', 'items_flattened') }}
GROUP BY
    customer_id