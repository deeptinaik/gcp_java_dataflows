{{ config(materialized='ephemeral') }}

-- Intermediate model: Ranked orders
-- Replicates the 'ranked_orders' CTE from the original BigQuery
-- Ranks orders by date for each customer

SELECT
    order_id,
    customer_id,
    order_date,
    SUM(quantity * price) AS order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
FROM 
    {{ ref('int_sales_aggregated') }}, 
    UNNEST(items)
GROUP BY
    order_id, 
    customer_id, 
    order_date