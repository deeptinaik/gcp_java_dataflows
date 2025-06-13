{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders with order totals
-- Preserves window function logic from BigQuery
SELECT
    order_id,
    customer_id,
    order_date,
    SUM(items_flattened.value:"quantity"::NUMBER * items_flattened.value:"price"::NUMBER) AS order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
FROM {{ ref('int_sales_with_items') }} sales,
    {{ unnest_array('sales.items', 'items_flattened') }}
GROUP BY
    order_id, customer_id, order_date