{{ config(materialized='view') }}

-- Staging model for orders data
-- Converts BigQuery nested structure to Snowflake format

WITH source_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        product_id,
        quantity,
        price
    FROM {{ source('source_system', 'orders') }}
),

-- Group orders with item aggregation similar to original BigQuery logic
orders_with_items AS (
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
    FROM source_orders
    GROUP BY
        order_id, customer_id, order_date
)

SELECT
    order_id,
    customer_id,
    order_date,
    items,
    {{ generate_etl_batch_id() }} AS etl_batch_id,
    {{ current_timestamp_snowflake() }} AS created_at
FROM orders_with_items