{{ config(
    materialized='table',
    schema='analytics_layer',
    post_hook="INSERT INTO {{ this.schema }}.customer_analysis_audit (execution_date, record_count, etl_batch_id) VALUES ('{{ run_started_at }}', (SELECT COUNT(*) FROM {{ this }}), '{{ generate_etl_batch_id() }}')"
) }}

-- Final mart model: Customer Analysis
-- Replicates the main SELECT from original BigQuery SQL
-- Provides comprehensive customer analysis with tier classification and recent order history

SELECT
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.first_order_date,
    c.last_order_date,
    c.avg_order_value,
    
    -- Customer tier classification using macro
    {{ classify_customer_tier('c.total_spent') }} AS customer_tier,
    
    -- Recent orders analysis (last 3 orders)
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', r.order_id,
            'order_total', r.order_total,
            'order_date', r.order_date,
            'order_rank', r.order_rank
        ) ORDER BY r.order_date DESC
    ) FILTER (WHERE r.order_rank <= 3) AS last_3_orders,
    
    -- Additional analysis fields
    DATEDIFF('day', c.first_order_date, c.last_order_date) AS customer_lifetime_days,
    CASE 
        WHEN DATEDIFF('day', c.last_order_date, CURRENT_DATE()) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', c.last_order_date, CURRENT_DATE()) <= 90 THEN 'At Risk'
        ELSE 'Inactive'
    END AS customer_status,
    
    -- ETL metadata
    {{ generate_current_timestamp() }} AS etl_created_at,
    '{{ generate_etl_batch_id() }}' AS etl_batch_id

FROM {{ ref('int_customer_totals') }} c
JOIN {{ ref('int_ranked_orders') }} r
    ON c.customer_id = r.customer_id
WHERE r.order_rank <= 3  -- Only include customers' last 3 orders for aggregation
GROUP BY
    c.customer_id,
    c.total_spent,
    c.total_orders,
    c.first_order_date,
    c.last_order_date,
    c.avg_order_value
ORDER BY
    c.total_spent DESC