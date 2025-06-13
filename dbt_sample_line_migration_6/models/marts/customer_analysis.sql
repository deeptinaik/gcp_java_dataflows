{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

-- Final customer analysis with tier classification
-- Replicates original BigQuery logic with Snowflake adaptations

WITH recent_orders AS (
    SELECT
        r.customer_id,
        {{ aggregate_order_details('r.order_id', 'r.order_total', 'r.order_date', var('max_recent_orders')) }} AS last_3_orders
    FROM {{ ref('stg_ranked_orders') }} r
    WHERE r.order_rank <= {{ var('max_recent_orders') }}
    GROUP BY r.customer_id
),

customer_analysis AS (
    SELECT
        c.customer_id,
        c.total_spent,
        c.total_orders,
        c.total_items,
        c.avg_order_value,
        c.min_order_value,
        c.max_order_value,
        ro.last_3_orders,
        {{ customer_tier_classification('c.total_spent') }} AS customer_tier,
        CASE 
            WHEN c.total_orders > 0 THEN 
                {{ safe_divide('c.total_spent', 'c.total_orders') }}
            ELSE 0 
        END AS spending_per_order,
        CASE
            WHEN c.total_spent > {{ var('vip_threshold') }} THEN 1
            ELSE 0
        END AS is_vip,
        CASE
            WHEN c.total_spent > {{ var('preferred_threshold') }} THEN 1
            ELSE 0
        END AS is_preferred_plus
    FROM {{ ref('stg_customer_totals') }} c
    LEFT JOIN recent_orders ro
        ON c.customer_id = ro.customer_id
)

SELECT
    customer_id,
    total_spent,
    total_orders,
    total_items,
    avg_order_value,
    min_order_value,
    max_order_value,
    last_3_orders,
    customer_tier,
    spending_per_order,
    is_vip,
    is_preferred_plus,
    {{ generate_etl_batch_id() }} AS etl_batch_id,
    {{ current_timestamp_snowflake() }} AS created_at,
    {{ current_timestamp_snowflake() }} AS updated_at
FROM customer_analysis
ORDER BY total_spent DESC