{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model that replicates the 'ranked_orders' CTE from original BigQuery
-- Ranks orders by date for each customer using Snowflake window functions

WITH sales_flattened AS (
    SELECT
        sales.order_id,
        sales.customer_id,
        sales.order_date,
        flattened_items.value:product_id::VARCHAR AS product_id,
        flattened_items.value:quantity::DECIMAL(10,2) AS quantity,
        flattened_items.value:price::DECIMAL(10,2) AS price
    FROM 
        {{ ref('stg_sales_orders') }} sales,
        LATERAL FLATTEN(input => sales.items) flattened_items
)

SELECT
    order_id,
    customer_id,
    order_date,
    SUM({{ calculate_order_total('quantity', 'price') }}) AS order_total,
    {{ rank_orders_by_date('customer_id', 'order_date') }} AS order_rank
FROM
    sales_flattened
GROUP BY
    order_id, customer_id, order_date