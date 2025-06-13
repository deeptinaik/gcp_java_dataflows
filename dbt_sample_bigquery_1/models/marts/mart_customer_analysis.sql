{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final mart model for customer analysis
-- Replicates BigQuery logic with Snowflake-compatible syntax
SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', r.order_id,
            'order_total', r.order_total,
            'order_date', r.order_date
        ) ORDER BY r.order_date DESC LIMIT 3
    ) AS last_3_orders,
    {{ classify_customer_tier('c.total_spent') }} AS customer_tier
FROM
    {{ ref('stg_customer_totals') }} c
JOIN
    {{ ref('stg_ranked_orders') }} r
ON
    c.customer_id = r.customer_id
WHERE
    r.order_rank <= 3
GROUP BY
    c.customer_id, c.total_spent, c.total_orders
ORDER BY
    c.total_spent DESC