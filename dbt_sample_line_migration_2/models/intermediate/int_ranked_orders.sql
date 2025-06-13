{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Orders with ranking by customer and date
-- Maintains BigQuery window function logic for order ranking
SELECT
    order_id,
    customer_id,
    order_date,
    SUM(item.value:quantity::NUMBER * item.value:price::NUMBER) AS order_total,
    {{ rank_orders_by_date('customer_id', 'order_date') }} AS order_rank
FROM {{ ref('int_sales_with_items') }} sales,
     LATERAL FLATTEN(input => sales.items) AS item
GROUP BY
    order_id, 
    customer_id, 
    order_date