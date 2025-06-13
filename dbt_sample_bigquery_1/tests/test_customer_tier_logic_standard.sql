-- Test that Standard customers have total_spent <= 5000
SELECT
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('mart_customer_analysis') }}
WHERE customer_tier = 'Standard' AND total_spent > 5000