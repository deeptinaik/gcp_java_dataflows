/*
  Final customer analysis model
  Combines customer totals with recent order details and tier classification
  Replicates the main SELECT from original BigQuery query
*/

{{ config(
    materialized='table',
    schema='analytics_layer',
    unique_key='customer_id',
    cluster_by=['customer_tier', 'total_spent'],
    post_hook="ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS etl_processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
) }}

WITH recent_orders AS (
  SELECT
    customer_id,
    {{ snowflake_array_agg_object('order_id', 'order_total', 'order_date') }} AS last_3_orders_json
  FROM {{ ref('int_ranked_orders') }}
  WHERE order_rank <= {{ var('max_recent_orders') }}
  GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.total_items_purchased,
    c.avg_item_value,
    c.last_order_date,
    c.first_order_date,
    r.last_3_orders_json,
    CASE
        WHEN c.total_spent > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN c.total_spent > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END AS customer_tier,
    DATEDIFF('day', c.first_order_date, c.last_order_date) AS customer_tenure_days,
    ROUND(c.total_spent / NULLIF(c.total_orders, 0), 2) AS avg_order_value,
    {{ generate_current_timestamp() }} AS etl_processed_timestamp
FROM {{ ref('int_customer_totals') }} c
LEFT JOIN recent_orders r 
    ON c.customer_id = r.customer_id
ORDER BY
    c.total_spent DESC