-- Mart model: Customer sales analysis with tier classification
-- Final business logic combining all staging models
-- Replaces the final SELECT from original BigQuery with Snowflake optimizations

{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

WITH recent_orders AS (
    SELECT
        customer_id,
        {{ aggregate_recent_orders(
            create_order_struct('order_id', 'order_total', 'order_date'),
            'order_date',
            3
        ) }} AS last_3_orders_array
    FROM
        {{ ref('stg_ranked_orders') }}
    WHERE
        order_rank <= 3
    GROUP BY
        customer_id
)

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.total_items,
    -- Get only first 3 elements from the array (equivalent to LIMIT 3 in BigQuery)
    ARRAY_SLICE(ro.last_3_orders_array, 0, 3) AS last_3_orders,
    -- Customer tier classification using macro
    {{ customer_tier_classification('c.total_spent') }} AS customer_tier,
    -- Additional analytics fields
    ROUND(c.total_spent / c.total_orders, 2) AS avg_order_value,
    ROUND(c.total_spent / c.total_items, 2) AS avg_item_value,
    -- ETL metadata
    {{ generate_current_timestamp() }} AS etl_created_timestamp,
    '{{ var("etl_batch_id") }}' AS etl_batch_id
FROM
    {{ ref('stg_customer_totals') }} c
LEFT JOIN
    recent_orders ro
ON
    c.customer_id = ro.customer_id
ORDER BY
    c.total_spent DESC