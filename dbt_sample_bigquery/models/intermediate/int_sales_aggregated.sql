{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales aggregated
-- Replicates the 'sales' CTE from the original BigQuery
-- Converts ARRAY_AGG(STRUCT()) to Snowflake equivalent

SELECT
    order_id,
    customer_id,
    order_date,
    -- Convert BigQuery ARRAY_AGG(STRUCT()) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT())
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) AS items
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date