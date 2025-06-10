{{
  config(
    materialized='table',
    description='Sales aggregated by customer tier per time window - equivalent to EcommercePipeline BigQuery output'
  )
}}

-- Equivalent to BigQuery table: your-project:analytics.sales_by_tier
-- Final output of EcommercePipeline.java aggregation

SELECT
    customer_tier,
    window_start,
    window_start + INTERVAL '{{ var("window_minutes") }} MINUTES' AS window_end,
    
    -- Aggregations (equivalent to Sum.doublesPerKey())
    SUM(amount) AS total_sales,
    COUNT(*) AS transaction_count,
    AVG(amount) AS avg_transaction_amount,
    MIN(amount) AS min_transaction_amount,
    MAX(amount) AS max_transaction_amount,
    
    COUNT(DISTINCT customer_id) AS unique_customers,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at

FROM {{ ref('int_purchase_events_enriched') }}
GROUP BY 
    customer_tier,
    window_start