{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales data aggregated by order
-- Equivalent to the 'sales' CTE in the original BigQuery

SELECT
    order_id,
    customer_id,
    order_date,
    -- Create array of order items using Snowflake OBJECT_CONSTRUCT and ARRAY_AGG
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) WITHIN GROUP (ORDER BY product_id) AS items,
    -- Calculate order totals
    SUM(line_total) AS order_total,
    COUNT(product_id) AS item_count
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date