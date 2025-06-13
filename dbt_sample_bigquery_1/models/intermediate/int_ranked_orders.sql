{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders by customer
-- Preserves BigQuery RANK() window function
SELECT
    order_id,
    customer_id,
    order_date,
    SUM(item_data.value:quantity::NUMBER * item_data.value:price::NUMBER) AS order_total,
    {{ rank_over('customer_id', 'order_date DESC') }} AS order_rank
FROM {{ ref('int_sales_with_items') }} sales,
     {{ unnest_array('sales', 'items') }} item_data
GROUP BY
    order_id, customer_id, order_date