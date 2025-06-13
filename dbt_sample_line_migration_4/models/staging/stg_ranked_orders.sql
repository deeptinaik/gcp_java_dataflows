-- Staging model: Ranked orders by customer
-- Replaces the 'ranked_orders' CTE from original BigQuery
-- Maintains window function logic for order ranking

{{ config(
    materialized='view',
    schema='staging_layer'
) }}

WITH flattened_items AS (
    SELECT
        s.order_id,
        s.customer_id,
        s.order_date,
        -- Use lateral flatten to replace BigQuery UNNEST
        f.value:quantity::NUMBER AS quantity,
        f.value:price::NUMBER AS price
    FROM
        {{ ref('stg_orders_aggregated') }} s,
        LATERAL FLATTEN(input => s.items) f
)

SELECT
    order_id,
    customer_id,
    order_date,
    SUM({{ calculate_line_total('quantity', 'price') }}) AS order_total,
    -- Window function for ranking orders by date (most recent first)
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_rank
FROM
    flattened_items
GROUP BY
    order_id, customer_id, order_date