{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Customer totals
-- Replicates the 'customer_totals' CTE from the original BigQuery
-- Converts UNNEST(items) to Snowflake LATERAL FLATTEN

SELECT
    customer_id,
    SUM(
        flattened_items.value:quantity::NUMBER * 
        flattened_items.value:price::NUMBER
    ) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM 
    {{ ref('int_sales_aggregated') }} sales,
    -- Convert BigQuery UNNEST(items) to Snowflake LATERAL FLATTEN
    LATERAL FLATTEN(input => sales.items) flattened_items
GROUP BY
    customer_id