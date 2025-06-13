-- Test that Preferred customers have total_spent between 5000 and 10000
SELECT
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('mart_customer_analysis') }}
WHERE customer_tier = 'Preferred' AND (total_spent <= 5000 OR total_spent > 10000)