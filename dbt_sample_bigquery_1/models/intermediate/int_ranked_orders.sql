{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders
-- Replicates the 'ranked_orders' CTE from original BigQuery SQL
-- Ranks orders by date per customer for recent order analysis

SELECT
    order_id,
    customer_id,
    order_date,
    order_total,
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_rank,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date DESC
    ) AS order_sequence
FROM {{ ref('int_sales_aggregated') }}