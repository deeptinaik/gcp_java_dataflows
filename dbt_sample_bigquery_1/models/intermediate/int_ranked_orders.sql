{{
  config(
    materialized='ephemeral'
  )
}}

-- Intermediate model: Ranked orders
-- Ranks orders by customer and date (similar to original ranked_orders CTE)
SELECT
    order_id,
    customer_id,
    order_date,
    order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_row_num
FROM {{ ref('int_sales_aggregated') }}