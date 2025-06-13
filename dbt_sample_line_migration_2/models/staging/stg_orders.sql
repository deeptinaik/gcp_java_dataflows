{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data - Clean and standardize source data
SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    -- Add derived fields for analysis
    quantity * price AS line_total,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS etl_processed_date,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id
FROM {{ source('raw_data', 'orders') }}
WHERE 
    -- Data quality filters
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity IS NOT NULL
    AND price IS NOT NULL
    AND quantity >= 0
    AND price > 0