{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Aggregated sales data per order
-- Replicates the 'sales' CTE from original BigQuery
-- Note: Snowflake handles arrays differently, so we'll use JSON for structured data
SELECT
    order_id,
    customer_id,
    order_date,
    -- Create JSON array of order items (equivalent to ARRAY_AGG(STRUCT()))
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price,
            'line_total', line_total
        )
    ) AS items_json,
    -- Also calculate order-level aggregates
    SUM(line_total) AS order_total,
    COUNT(*) AS item_count
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date