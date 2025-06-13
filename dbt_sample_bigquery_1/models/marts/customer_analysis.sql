{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final customer analysis model that combines all staging data
-- Replicates the final SELECT from original BigQuery with Snowflake optimizations

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', r.order_id,
            'order_total', r.order_total,
            'order_date', r.order_date
        )
        ORDER BY r.order_date DESC
        LIMIT {{ var('max_recent_orders') }}
    ) AS last_3_orders,
    {{ calculate_customer_tier('c.total_spent') }} AS customer_tier
FROM
    {{ ref('stg_customer_totals') }} c
JOIN
    {{ ref('stg_ranked_orders') }} r
ON
    c.customer_id = r.customer_id
WHERE
    {{ filter_recent_orders('r.order_rank', var('max_recent_orders')) }}
GROUP BY
    c.customer_id, c.total_spent, c.total_orders
ORDER BY
    c.total_spent DESC