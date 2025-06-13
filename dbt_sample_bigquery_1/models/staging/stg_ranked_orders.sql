{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for ranked orders
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN
WITH sales_flattened AS (
    SELECT
        s.order_id,
        s.customer_id,
        s.order_date,
        f.value:product_id::VARCHAR AS product_id,
        f.value:quantity::NUMBER AS quantity,
        f.value:price::NUMBER AS price
    FROM {{ ref('stg_sales') }} s,
    LATERAL FLATTEN(INPUT => s.items) f
)
SELECT
    order_id,
    customer_id,
    order_date,
    SUM({{ calculate_order_total('quantity', 'price') }}) AS order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
FROM sales_flattened
GROUP BY
    order_id, customer_id, order_date