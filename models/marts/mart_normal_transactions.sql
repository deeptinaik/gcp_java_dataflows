{{
  config(
    materialized='table',
    description='Normal transactions for monitoring and analytics - equivalent to FraudDetectionPipeline BigQuery output'
  )
}}

-- Equivalent to BigQuery table: your-project:fraud.normal
-- Final output of FraudDetectionPipeline.java for normal transactions

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_timestamp,
    location,
    
    customer_avg_amount,
    customer_total_transactions,
    customer_risk_score,
    
    -- Transaction classification
    fraud_status,
    amount / customer_avg_amount AS amount_vs_avg_ratio,
    
    -- Additional analysis fields
    CASE
      WHEN amount_vs_avg_ratio > 2 THEN 'ELEVATED'
      WHEN amount_vs_avg_ratio > 1.5 THEN 'ABOVE_AVERAGE'
      ELSE 'NORMAL'
    END AS transaction_pattern,
    
    -- Metadata
    CURRENT_TIMESTAMP AS processed_at,
    _loaded_at

FROM {{ ref('int_transactions_enriched') }}
WHERE fraud_status = 'NORMAL'