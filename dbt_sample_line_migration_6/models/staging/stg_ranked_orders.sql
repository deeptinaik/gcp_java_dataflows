{{ config(materialized='view') }}

-- Ranked orders with order totals and ranking logic
-- Converts BigQuery window function to Snowflake equivalent

WITH orders_flattened AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        item.value:quantity::DECIMAL(10,2) AS quantity,
        item.value:price::DECIMAL(10,2) AS price
    FROM {{ ref('stg_orders') }} o,
    LATERAL FLATTEN(input => o.items) AS item
),

order_totals AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        SUM(quantity * price) AS order_total
    FROM orders_flattened
    GROUP BY
        order_id, customer_id, order_date
),

ranked_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_total,
        RANK() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date DESC
        ) AS order_rank,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date DESC
        ) AS order_row_num
    FROM order_totals
)

SELECT
    order_id,
    customer_id,
    order_date,
    {{ format_currency('order_total') }} AS order_total,
    order_rank,
    order_row_num,
    {{ generate_etl_batch_id() }} AS etl_batch_id,
    {{ current_timestamp_snowflake() }} AS created_at
FROM ranked_orders