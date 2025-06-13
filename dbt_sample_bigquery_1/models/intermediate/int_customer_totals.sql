{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals calculation
-- Replicates the original "customer_totals" CTE logic using Snowflake LATERAL FLATTEN
SELECT
    customer_id,
    SUM(item_data.value:quantity::NUMBER * item_data.value:price::NUMBER) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('int_sales_aggregated') }}
    , LATERAL FLATTEN(input => items) AS item_data
GROUP BY
    customer_id