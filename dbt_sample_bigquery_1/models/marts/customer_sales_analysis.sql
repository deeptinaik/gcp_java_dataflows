{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final mart model: Customer sales analysis
-- Replicates the original final SELECT logic with Snowflake-compatible syntax
SELECT
    c.customer_id,
    {{ format_currency_amount('c.total_spent') }} AS total_spent,
    c.total_orders,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', r.order_id,
            'order_total', {{ format_currency_amount('r.order_total') }},
            'order_date', r.order_date
        )
        ORDER BY r.order_date DESC
    ) WITHIN GROUP (ORDER BY r.order_date DESC) AS last_3_orders,
    {{ calculate_customer_tier('c.total_spent') }} AS customer_tier,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS dbt_created_at,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id
FROM {{ ref('int_customer_totals') }} c
JOIN {{ ref('int_ranked_orders') }} r
    ON c.customer_id = r.customer_id
WHERE r.order_rank <= 3
GROUP BY
    c.customer_id, c.total_spent, c.total_orders
ORDER BY
    c.total_spent DESC