{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders source data
-- Maps BigQuery project.dataset.orders to Snowflake source
SELECT 
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price
FROM {{ source('sales_data', 'orders') }}