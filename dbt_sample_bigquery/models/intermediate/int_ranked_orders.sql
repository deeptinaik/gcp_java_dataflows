{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders by customer
-- Replicates the 'ranked_orders' CTE from original BigQuery
SELECT
    order_id,
    customer_id,
    order_date,
    order_total,
    item_count,
    items_json,
    -- Ranking logic from original query
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_rank,
    -- Additional useful rankings
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_sequence,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_total DESC
    ) AS order_value_rank
FROM {{ ref('int_sales_aggregated') }}