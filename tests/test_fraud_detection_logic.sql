-- Test that fraud detection logic works correctly
-- Suspicious transactions should have amount > 3 * customer_avg_amount

SELECT
    transaction_id,
    amount,
    customer_avg_amount,
    amount / customer_avg_amount AS ratio
FROM {{ ref('mart_suspicious_transactions') }}
WHERE amount <= ({{ var('fraud_threshold_multiplier') }} * customer_avg_amount)