{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final mart model: Customer analytics with recent orders and tier classification
-- Converts complex BigQuery aggregations to Snowflake equivalent structures
SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    -- Convert BigQuery ARRAY_AGG with STRUCT and ORDER BY LIMIT to Snowflake
    {{ create_recent_orders_array('r.order_id', 'r.order_total', 'r.order_date', var('max_recent_orders')) }} AS last_3_orders,
    -- Customer tier classification using business logic macro
    {{ classify_customer_tier('c.total_spent') }} AS customer_tier,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS created_date,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id
FROM
    {{ ref('int_customer_totals') }} c
JOIN
    {{ ref('int_ranked_orders') }} r
ON
    c.customer_id = r.customer_id
WHERE
    r.order_rank <= {{ var('max_recent_orders') }}
GROUP BY
    c.customer_id, 
    c.total_spent, 
    c.total_orders
ORDER BY
    c.total_spent DESC