{{ config(
    materialized='table',
    schema='customer_analytics'
) }}

-- Final customer analysis model
-- Migrated from sample_bigquery.sql with complete business logic preservation
SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    {{ array_agg_struct(
        "'order_id', r.order_id, 'order_total', r.order_total, 'order_date', r.order_date", 
        "r.order_date DESC"
    ) }} AS last_3_orders,
    {{ classify_customer_tier('c.total_spent') }} AS customer_tier
FROM {{ ref('int_customer_totals') }} c
JOIN {{ ref('int_ranked_orders') }} r
    ON c.customer_id = r.customer_id
WHERE r.order_rank <= {{ var('recent_orders_limit') }}
GROUP BY
    c.customer_id, c.total_spent, c.total_orders
ORDER BY
    c.total_spent DESC