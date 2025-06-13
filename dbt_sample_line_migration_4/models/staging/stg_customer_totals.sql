-- Staging model: Customer totals calculation
-- Replaces the 'customer_totals' CTE from original BigQuery
-- Uses lateral flatten to replace UNNEST functionality

{{ config(
    materialized='view',
    schema='staging_layer'
) }}

WITH flattened_items AS (
    SELECT
        s.customer_id,
        s.order_id,
        -- Use lateral flatten to replace BigQuery UNNEST
        f.value:quantity::NUMBER AS quantity,
        f.value:price::NUMBER AS price
    FROM
        {{ ref('stg_orders_aggregated') }} s,
        LATERAL FLATTEN(input => s.items) f
)

SELECT
    customer_id,
    SUM({{ calculate_line_total('quantity', 'price') }}) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items
FROM
    flattened_items
GROUP BY
    customer_id