{{ config(materialized='view') }}

-- Staging model for orders data
-- This model provides clean, standardized access to raw orders data

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price
FROM 
    {{ var('source_project') }}.{{ var('source_dataset') }}.orders

-- Add basic data quality filters
WHERE 
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND quantity > 0
    AND price >= 0