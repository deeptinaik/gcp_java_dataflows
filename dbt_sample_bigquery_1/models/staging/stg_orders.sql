{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Converts BigQuery source reference to Snowflake format
-- and prepares data for downstream transformations

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    quantity * price AS line_total,
    {{ generate_current_timestamp() }} AS etl_created_at,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id
FROM {{ ref('sample_orders') }}
WHERE order_date IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_id IS NOT NULL