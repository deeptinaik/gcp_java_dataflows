{% macro apply_complex_fraud_rules(transaction_amount, location, merchant_id=none, customer_risk_score=none) %}
  -- Equivalent to complex fraud detection pipeline rules
  -- Combines FraudRuleA, FraudRuleB, FraudRuleC logic
  
  CASE
    -- Rule A: High amount transactions (FraudRuleA.java)
    WHEN {{ transaction_amount }} > 10000 THEN 'RULE_A_HIGH_AMOUNT'
    
    -- Rule B: Blacklisted location (FraudRuleB.java)  
    WHEN {{ location }} = 'blacklisted' THEN 'RULE_B_BLACKLISTED_LOCATION'
    
    -- Rule C: High risk merchant (FraudRuleC equivalent)
    {% if merchant_id %}
    WHEN {{ merchant_id }} IN ('MERCHANT_001', 'MERCHANT_999', 'MERCHANT_SUSPICIOUS') THEN 'RULE_C_HIGH_RISK_MERCHANT'
    {% endif %}
    
    -- Additional rules based on customer risk score
    {% if customer_risk_score %}
    WHEN {{ customer_risk_score }} > 8 AND {{ transaction_amount }} > 1000 THEN 'RULE_D_HIGH_RISK_CUSTOMER'
    {% endif %}
    
    -- Combination rules
    WHEN {{ transaction_amount }} > 5000 AND {{ location }} LIKE '%foreign%' THEN 'RULE_E_HIGH_AMOUNT_FOREIGN'
    
    ELSE 'NO_RULE_TRIGGERED'
  END
{% endmacro %}