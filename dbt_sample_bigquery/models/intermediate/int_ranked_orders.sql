{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders
-- Replicates the 'ranked_orders' CTE from the original BigQuery
-- Converts UNNEST(items) to Snowflake LATERAL FLATTEN

SELECT
    order_id,
    customer_id,
    order_date,
    SUM(
        flattened_items.value:quantity::NUMBER * 
        flattened_items.value:price::NUMBER
    ) AS order_total,
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_rank
FROM 
    {{ ref('int_sales_aggregated') }} sales,
    -- Convert BigQuery UNNEST(items) to Snowflake LATERAL FLATTEN
    LATERAL FLATTEN(input => sales.items) flattened_items
GROUP BY
    order_id, 
    customer_id, 
    order_date