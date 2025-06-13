{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Ranked orders calculation
-- Replicates the original "ranked_orders" CTE logic with Snowflake window functions
SELECT
    order_id,
    customer_id,
    order_date,
    SUM(item_data.value:quantity::NUMBER * item_data.value:price::NUMBER) AS order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
FROM {{ ref('int_sales_aggregated') }}
    , LATERAL FLATTEN(input => items) AS item_data
GROUP BY
    order_id, customer_id, order_date