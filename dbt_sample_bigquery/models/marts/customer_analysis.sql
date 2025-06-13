{{ config(
    materialized='table',
    schema='analytics_layer',
    indexes=[
      {'columns': ['customer_id'], 'unique': true},
      {'columns': ['customer_tier']},
      {'columns': ['total_spent']}
    ]
) }}

-- Final marts model: Customer Analysis
-- Replicates the final SELECT from the original BigQuery query
-- Combines customer totals with recent order history and tier classification

WITH recent_orders_agg AS (
  SELECT
    customer_id,
    {{ aggregate_recent_orders('items_json', 'order_date', 'order_total', var('max_recent_orders')) }} AS last_orders_json
  FROM {{ ref('int_ranked_orders') }}
  WHERE order_rank <= {{ var('max_recent_orders') }}
  GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.total_items,
    c.avg_item_value,
    c.first_order_date,
    c.last_order_date,
    
    -- Recent orders in JSON format (equivalent to original ARRAY_AGG(STRUCT(...)))
    COALESCE(ro.last_orders_json, ARRAY_CONSTRUCT()) AS last_3_orders,
    
    -- Customer tier classification using macro
    {{ customer_tier_classification('c.total_spent') }} AS customer_tier,
    
    -- Additional business metrics
    DATEDIFF('day', c.first_order_date, c.last_order_date) AS customer_lifetime_days,
    CASE 
        WHEN DATEDIFF('day', c.first_order_date, c.last_order_date) > 0 
        THEN c.total_spent / DATEDIFF('day', c.first_order_date, c.last_order_date)
        ELSE c.total_spent
    END AS avg_daily_spend,
    
    -- Audit fields
    {{ generate_current_timestamp() }} AS analysis_timestamp,
    '{{ run_started_at.strftime("%Y%m%d%H%M%S") }}' AS etl_batch_id

FROM {{ ref('int_customer_totals') }} c
LEFT JOIN recent_orders_agg ro
    ON c.customer_id = ro.customer_id

-- Order by total spent descending (replicating original ORDER BY)
ORDER BY c.total_spent DESC