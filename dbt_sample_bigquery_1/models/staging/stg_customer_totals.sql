{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for customer totals
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN
WITH sales_flattened AS (
    SELECT
        s.customer_id,
        f.value:product_id::VARCHAR AS product_id,
        f.value:quantity::NUMBER AS quantity,
        f.value:price::NUMBER AS price,
        s.order_id
    FROM {{ ref('stg_sales') }} s,
    LATERAL FLATTEN(INPUT => s.items) f
)
SELECT
    customer_id,
    SUM({{ calculate_order_total('quantity', 'price') }}) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM sales_flattened
GROUP BY
    customer_id