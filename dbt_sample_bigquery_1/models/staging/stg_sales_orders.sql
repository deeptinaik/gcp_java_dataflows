{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model that replicates the 'sales' CTE from original BigQuery
-- Groups orders and aggregates product items using Snowflake syntax

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
FROM
    {{ source('source_data', 'orders') }}
GROUP BY
    order_id, customer_id, order_date