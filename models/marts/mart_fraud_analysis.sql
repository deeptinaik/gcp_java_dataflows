{{
  config(
    materialized='table',
    description='Comprehensive fraud analysis with all rule triggers - equivalent to complex fraud detection pipeline'
  )
}}

-- All transactions with detailed fraud rule analysis
-- Equivalent to complex_fraud_detection_pipeline output

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_timestamp,
    location,
    
    -- Customer profile information
    customer_avg_amount,
    customer_total_transactions,
    customer_risk_score,
    customer_existing_tier,
    
    -- Fraud detection results
    fraud_status,
    fraud_rule_triggered,
    
    -- Individual rule flags (for detailed analysis)
    CASE WHEN fraud_rule_triggered = 'RULE_A_HIGH_AMOUNT' THEN TRUE ELSE FALSE END AS rule_a_high_amount,
    CASE WHEN fraud_rule_triggered = 'RULE_B_BLACKLISTED_LOCATION' THEN TRUE ELSE FALSE END AS rule_b_blacklisted_location,
    CASE WHEN fraud_rule_triggered = 'RULE_C_HIGH_RISK_MERCHANT' THEN TRUE ELSE FALSE END AS rule_c_high_risk_merchant,
    CASE WHEN fraud_rule_triggered = 'RULE_D_HIGH_RISK_CUSTOMER' THEN TRUE ELSE FALSE END AS rule_d_high_risk_customer,
    CASE WHEN fraud_rule_triggered = 'RULE_E_HIGH_AMOUNT_FOREIGN' THEN TRUE ELSE FALSE END AS rule_e_high_amount_foreign,
    
    -- Risk scoring
    CASE 
      WHEN fraud_rule_triggered LIKE 'RULE_A%' THEN 9
      WHEN fraud_rule_triggered LIKE 'RULE_B%' THEN 10
      WHEN fraud_rule_triggered LIKE 'RULE_C%' THEN 8
      WHEN fraud_rule_triggered LIKE 'RULE_D%' THEN 7
      WHEN fraud_rule_triggered LIKE 'RULE_E%' THEN 6
      ELSE 0
    END AS rule_risk_score,
    
    -- Overall risk assessment
    CASE
      WHEN fraud_status = 'SUSPICIOUS' AND fraud_rule_triggered != 'NO_RULE_TRIGGERED' THEN 'HIGH_RISK'
      WHEN fraud_status = 'SUSPICIOUS' OR fraud_rule_triggered != 'NO_RULE_TRIGGERED' THEN 'MEDIUM_RISK'
      ELSE 'LOW_RISK'
    END AS overall_risk_level,
    
    -- Analysis metrics
    amount / customer_avg_amount AS amount_vs_avg_ratio,
    
    -- Metadata
    CURRENT_TIMESTAMP AS analyzed_at,
    _loaded_at

FROM {{ ref('int_transactions_enriched') }}