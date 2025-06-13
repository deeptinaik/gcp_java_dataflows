{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for orders data
-- Cleans and standardizes order data from the source table
SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    (quantity * price) AS line_total
FROM {{ source('raw_data', 'orders') }}
WHERE 
    order_id IS NOT NULL 
    AND customer_id IS NOT NULL 
    AND order_date IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0
    AND price > 0