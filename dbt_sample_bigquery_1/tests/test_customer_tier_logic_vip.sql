-- Test that VIP customers have total_spent > 10000
SELECT
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('mart_customer_analysis') }}
WHERE customer_tier = 'VIP' AND total_spent <= 10000