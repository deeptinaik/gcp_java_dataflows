{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for orders data
-- Maps BigQuery source table to Snowflake structure
-- Uses seed data for development, can be switched to source for production
SELECT 
    order_id,
    customer_id,
    TRY_TO_DATE(order_date, 'YYYY-MM-DD') as order_date,
    product_id,
    quantity,
    price
FROM {{ ref('sample_orders_data') }}