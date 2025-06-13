{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Converts BigQuery source reference to Snowflake format and provides clean interface
SELECT 
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS dbt_loaded_at,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id
FROM {{ source('sales_data', 'orders') }}