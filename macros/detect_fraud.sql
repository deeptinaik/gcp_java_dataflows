{% macro detect_fraud(transaction_amount, customer_avg_amount) %}
  -- Equivalent to FraudDetectionPipeline.java logic
  -- if (tx.amount > 3 * avg) { suspicious } else { normal }
  
  CASE
    WHEN {{ transaction_amount }} > ({{ var('fraud_threshold_multiplier') }} * {{ customer_avg_amount }}) THEN 'SUSPICIOUS'
    ELSE 'NORMAL'
  END
{% endmacro %}