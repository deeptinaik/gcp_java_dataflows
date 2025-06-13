{{
  config(
    materialized='table',
    schema='analytics_layer'
  )
}}

-- Final customer analysis model
-- Replicates the complete logic from the original BigQuery sample_bigquery_1.sql
WITH recent_orders AS (
  SELECT
    customer_id,
    {{ create_order_summary_array('order_id', 'order_total', 'order_date', var('max_recent_orders')) }} AS last_3_orders
  FROM {{ ref('int_ranked_orders') }}
  WHERE order_rank <= {{ var('max_recent_orders') }}
  GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.total_line_items,
    c.avg_order_value,
    c.first_order_date,
    c.last_order_date,
    r.last_3_orders,
    {{ calculate_customer_tier('c.total_spent') }} AS customer_tier,
    DATEDIFF('day', c.first_order_date, c.last_order_date) AS customer_lifetime_days,
    {{ generate_current_timestamp() }} AS analysis_timestamp,
    {{ generate_etl_batch_id() }} AS etl_batch_id
FROM {{ ref('int_customer_totals') }} c
LEFT JOIN recent_orders r
    ON c.customer_id = r.customer_id
ORDER BY c.total_spent DESC