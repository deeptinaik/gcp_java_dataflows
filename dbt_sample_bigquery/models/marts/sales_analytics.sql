{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Sales Analytics: Customer analysis with tier classification and order history
SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    {{ get_last_n_orders('r.order_id', 'r.order_total', 'r.order_date', var('max_recent_orders')) }} AS last_3_orders,
    {{ customer_tier_classification('c.total_spent') }} AS customer_tier
FROM {{ ref('int_customer_totals') }} c
JOIN {{ ref('int_ranked_orders') }} r
    ON c.customer_id = r.customer_id
WHERE r.order_rank <= {{ var('max_recent_orders') }}
GROUP BY
    c.customer_id, 
    c.total_spent, 
    c.total_orders
ORDER BY
    c.total_spent DESC