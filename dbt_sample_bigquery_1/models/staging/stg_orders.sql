{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Abstracts source data and provides clean foundation for transformations

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    -- Calculate line totals for easier downstream processing
    quantity * price AS line_total,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS created_at,
    {{ generate_etl_batch_id() }} AS etl_batch_id
FROM {{ source('source_data', 'orders') }}
WHERE 
    -- Data quality filters
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0
    AND price >= 0