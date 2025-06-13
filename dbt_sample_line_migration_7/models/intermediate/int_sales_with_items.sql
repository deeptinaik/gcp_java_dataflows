{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales with aggregated items
-- Converts BigQuery ARRAY_AGG(STRUCT()) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT())
SELECT
    order_id,
    customer_id,
    order_date,
    {{ array_agg_struct("'product_id', product_id, 'quantity', quantity, 'price', price", "product_id") }} AS items
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, customer_id, order_date