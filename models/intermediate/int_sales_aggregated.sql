{{ config(materialized='ephemeral') }}

-- Intermediate model: Sales aggregation
-- Replicates the 'sales' CTE from the original BigQuery
-- Groups orders and creates structured item arrays

SELECT
    order_id,
    customer_id,
    order_date,
    ARRAY_AGG(STRUCT(product_id, quantity, price)) AS items
FROM 
    {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date