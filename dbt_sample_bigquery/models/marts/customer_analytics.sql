{{ config(
    materialized='table',
    unique_key='customer_id',
    schema='analytics_layer'
) }}

-- Final marts model: Customer analytics
-- Replicates the final SELECT from the original BigQuery
-- Provides comprehensive customer analysis with order history and tier classification

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    -- Convert BigQuery ARRAY_AGG(STRUCT()) to Snowflake equivalent for last 3 orders
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', r.order_id,
            'order_total', r.order_total,
            'order_date', r.order_date
        )
    ) WITHIN GROUP (ORDER BY r.order_date DESC) AS last_3_orders,
    -- Use macro for customer tier classification
    {{ classify_customer_tier('c.total_spent') }} AS customer_tier,
    -- Add metadata columns for operational tracking
    {{ current_datetime() }} AS created_at,
    '{{ var("etl_batch_date") }}' AS batch_date
FROM
    {{ ref('int_customer_totals') }} c
JOIN
    {{ ref('int_ranked_orders') }} r
ON
    c.customer_id = r.customer_id
WHERE
    r.order_rank <= {{ var('max_last_orders') }}
GROUP BY
    c.customer_id, 
    c.total_spent, 
    c.total_orders
ORDER BY
    c.total_spent DESC