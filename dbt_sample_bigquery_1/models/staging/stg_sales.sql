{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for sales data with item aggregation
-- Converts BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT(...))
SELECT
    order_id,
    customer_id,
    order_date,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) AS items
FROM {{ source('source_database', 'orders') }}
GROUP BY
    order_id, customer_id, order_date