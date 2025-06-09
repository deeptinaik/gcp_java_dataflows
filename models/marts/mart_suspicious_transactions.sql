{{
  config(
    materialized='table',
    description='Suspicious transactions identified by fraud detection - equivalent to FraudDetectionPipeline BigQuery output'
  )
}}

-- Equivalent to BigQuery table: your-project:fraud.suspicious
-- Final output of FraudDetectionPipeline.java for suspicious transactions

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_timestamp,
    location,
    
    customer_avg_amount,
    customer_total_transactions,
    customer_risk_score,
    
    -- Fraud detection metadata
    fraud_status,
    amount / customer_avg_amount AS amount_vs_avg_ratio,
    
    -- Additional fraud indicators (equivalent to complex fraud rules)
    CASE WHEN location = 'blacklisted' THEN TRUE ELSE FALSE END AS blacklisted_location,
    CASE WHEN amount > 10000 THEN TRUE ELSE FALSE END AS high_amount_flag,
    
    -- Metadata
    CURRENT_TIMESTAMP AS detected_at,
    _loaded_at

FROM {{ ref('int_transactions_enriched') }}
WHERE fraud_status = 'SUSPICIOUS'