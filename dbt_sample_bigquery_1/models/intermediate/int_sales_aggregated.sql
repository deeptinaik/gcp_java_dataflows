{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales data with items aggregated per order
-- Replicates the original "sales" CTE logic with Snowflake array aggregation
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
    ) AS items
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, customer_id, order_date