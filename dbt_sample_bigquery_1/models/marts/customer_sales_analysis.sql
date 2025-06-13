{{ config(
    materialized='table',
    schema='analytics_layer'
) }}

-- Final mart model: Customer Sales Analysis
-- Equivalent to the final SELECT in the original BigQuery query
-- Combines customer totals with last 3 orders and customer tier classification

WITH last_3_orders AS (
    SELECT
        customer_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'order_id', order_id,
                'order_total', order_total,
                'order_date', order_date
            )
        ) WITHIN GROUP (ORDER BY order_date DESC) AS last_3_orders
    FROM {{ ref('int_ranked_orders') }}
    WHERE order_rank <= 3
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.total_items,
    c.avg_line_value,
    c.first_order_date,
    c.last_order_date,
    lo.last_3_orders,
    {{ calculate_customer_tier('c.total_spent') }} AS customer_tier,
    -- Add audit fields
    {{ generate_current_timestamp() }} AS created_at,
    {{ generate_etl_batch_id() }} AS etl_batch_id
FROM {{ ref('int_customer_totals') }} c
LEFT JOIN last_3_orders lo 
    ON c.customer_id = lo.customer_id
ORDER BY
    c.total_spent DESC