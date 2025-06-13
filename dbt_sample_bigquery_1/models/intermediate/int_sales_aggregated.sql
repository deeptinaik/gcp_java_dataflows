{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales aggregated
-- Replicates the 'sales' CTE from original BigQuery SQL
-- Aggregates order items into structured objects for each order

SELECT
    order_id,
    customer_id,
    order_date,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) AS items,
    SUM(quantity * price) AS order_total,
    COUNT(product_id) AS item_count
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id,
    customer_id,
    order_date