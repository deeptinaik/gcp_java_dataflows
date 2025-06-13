{{ config(materialized='view') }}

-- Customer totals calculation from orders data
-- Replaces BigQuery UNNEST operation with Snowflake LATERAL FLATTEN

WITH orders_flattened AS (
    SELECT
        o.customer_id,
        item.value:product_id::STRING AS product_id,
        item.value:quantity::DECIMAL(10,2) AS quantity,
        item.value:price::DECIMAL(10,2) AS price,
        o.order_id
    FROM {{ ref('stg_orders') }} o,
    LATERAL FLATTEN(input => o.items) AS item
),

customer_aggregations AS (
    SELECT
        customer_id,
        SUM(quantity * price) AS total_spent,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_items,
        AVG(quantity * price) AS avg_order_value,
        MIN(quantity * price) AS min_order_value,
        MAX(quantity * price) AS max_order_value
    FROM orders_flattened
    GROUP BY customer_id
)

SELECT
    customer_id,
    {{ format_currency('total_spent') }} AS total_spent,
    total_orders,
    total_items,
    {{ format_currency('avg_order_value') }} AS avg_order_value,
    {{ format_currency('min_order_value') }} AS min_order_value,
    {{ format_currency('max_order_value') }} AS max_order_value,
    {{ generate_etl_batch_id() }} AS etl_batch_id,
    {{ current_timestamp_snowflake() }} AS created_at
FROM customer_aggregations