{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data with product aggregation
SELECT
    order_id,
    customer_id,
    order_date,
    {{ aggregate_order_items('product_id', 'quantity', 'price') }} AS items
FROM {{ source('source_data', 'orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date