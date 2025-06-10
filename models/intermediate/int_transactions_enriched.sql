{{
  config(
    materialized='table',
    description='Deduplicated transactions enriched with customer profiles and fraud detection'
  )
}}

-- Equivalent to FraudDetectionPipeline.java processing:
-- 1. Deduplicate transactions
-- 2. Enrich with customer profiles  
-- 3. Detect fraud

WITH deduplicated_transactions AS (
  {{ deduplicate_by_key('ref("stg_transactions")', 'transaction_id', 'transaction_timestamp', var('deduplication_hours')) }}
),

enriched_transactions AS (
  SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.transaction_timestamp,
    t.location,
    
    -- Enrich with customer profile (equivalent to side input lookup)
    cp.avg_amount AS customer_avg_amount,
    cp.total_transactions AS customer_total_transactions,
    cp.risk_score AS customer_risk_score,
    cp.customer_tier AS customer_existing_tier,
    
    -- Detect fraud using macro (equivalent to fraud detection logic)
    {{ detect_fraud('t.amount', 'cp.avg_amount') }} AS fraud_status,
    
    -- Apply complex fraud rules (equivalent to complex fraud detection pipeline)
    {{ apply_complex_fraud_rules('t.amount', 't.location', none, 'cp.risk_score') }} AS fraud_rule_triggered,
    
    t._loaded_at

  FROM deduplicated_transactions t
  LEFT JOIN {{ ref('stg_customer_profiles') }} cp
    ON t.customer_id = cp.customer_id
)

SELECT * FROM enriched_transactions