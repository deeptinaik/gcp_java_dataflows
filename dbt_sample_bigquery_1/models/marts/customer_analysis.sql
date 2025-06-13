{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final mart model: Customer analysis with sales data
-- Replicates the complete logic from sample_bigquery_1.sql
SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    {{ array_agg_struct([
        {'name': 'order_id', 'expr': 'r.order_id'},
        {'name': 'order_total', 'expr': 'r.order_total'},
        {'name': 'order_date', 'expr': 'r.order_date'}
    ], 'r.order_date DESC', var('max_recent_orders')) }} AS last_3_orders,
    {{ customer_tier_classification('c.total_spent') }} AS customer_tier
FROM {{ ref('int_customer_totals') }} c
JOIN {{ ref('int_ranked_orders') }} r
    ON c.customer_id = r.customer_id
WHERE r.order_rank <= {{ var('max_recent_orders') }}
GROUP BY
    c.customer_id, c.total_spent, c.total_orders
ORDER BY
    c.total_spent DESC