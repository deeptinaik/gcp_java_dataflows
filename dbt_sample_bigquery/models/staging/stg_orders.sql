{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Converts BigQuery source reference to Snowflake source
SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    -- Calculate line total for each order item
    quantity * price AS line_total,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS loaded_at
FROM {{ source('raw_data', 'orders') }}
WHERE 
    -- Basic data quality filters
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND quantity > 0
    AND price >= 0