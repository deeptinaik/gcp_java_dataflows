{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model that replicates the 'customer_totals' CTE from original BigQuery
-- Calculates customer spending totals using Snowflake FLATTEN function

WITH sales_flattened AS (
    SELECT
        sales.customer_id,
        sales.order_id,
        flattened_items.value:product_id::VARCHAR AS product_id,
        flattened_items.value:quantity::DECIMAL(10,2) AS quantity,
        flattened_items.value:price::DECIMAL(10,2) AS price
    FROM 
        {{ ref('stg_sales_orders') }} sales,
        LATERAL FLATTEN(input => sales.items) flattened_items
)

SELECT
    customer_id,
    SUM({{ calculate_order_total('quantity', 'price') }}) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
FROM
    sales_flattened
GROUP BY
    customer_id