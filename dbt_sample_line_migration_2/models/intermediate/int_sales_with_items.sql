{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales data with aggregated items array
-- Converts BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake arrays and objects
SELECT
    order_id,
    customer_id,
    order_date,
    -- Convert BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake equivalent
    {{ create_order_items_array('product_id', 'quantity', 'price') }} AS items
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date