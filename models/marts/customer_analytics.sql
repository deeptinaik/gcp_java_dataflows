{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

-- Final customer analytics model
-- Combines all intermediate models to replicate the original BigQuery logic
-- Provides comprehensive customer insights with order history and tier classification

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    ARRAY_AGG(
        STRUCT(r.order_id, r.order_total, r.order_date) 
        ORDER BY r.order_date DESC 
        LIMIT 3
    ) AS last_3_orders,
    {{ get_customer_tier('c.total_spent') }} AS customer_tier
FROM 
    {{ ref('int_customer_totals') }} c
JOIN 
    {{ ref('int_ranked_orders') }} r
ON 
    c.customer_id = r.customer_id
WHERE 
    r.order_rank <= 3
GROUP BY
    c.customer_id, 
    c.total_spent, 
    c.total_orders
ORDER BY
    c.total_spent DESC