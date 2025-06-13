{{
  config(
    materialized='ephemeral'
  )
}}

-- Intermediate model: Sales aggregated by order
-- Creates order-level aggregations with item arrays similar to BigQuery ARRAY_AGG(STRUCT(...))
SELECT
    order_id,
    customer_id,
    order_date,
    {{ create_order_items_array('product_id', 'quantity', 'price') }} AS items,
    SUM(line_total) AS order_total,
    COUNT(*) AS item_count
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, customer_id, order_date