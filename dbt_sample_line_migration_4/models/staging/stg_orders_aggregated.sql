-- Staging model: Orders with aggregated items
-- Replaces the 'sales' CTE from original BigQuery
-- Converts ARRAY_AGG(STRUCT()) to Snowflake equivalent

{{ config(
    materialized='view',
    schema='staging_layer'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    -- Convert BigQuery ARRAY_AGG(STRUCT()) to Snowflake ARRAY_AGG with OBJECT_CONSTRUCT
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) AS items,
    -- Additional calculated fields for downstream use
    SUM({{ calculate_line_total('quantity', 'price') }}) AS order_total,
    COUNT(*) AS item_count
FROM
    {{ source('raw_data', 'orders') }}
GROUP BY
    order_id, customer_id, order_date