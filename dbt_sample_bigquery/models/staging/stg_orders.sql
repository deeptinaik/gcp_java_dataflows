{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Prepares raw order data for downstream transformations
-- Replicates the source data access from the original BigQuery

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price
FROM {{ source('trusted_layer', 'orders') }}
WHERE 
    -- Data quality filters
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0
    AND price >= 0