-- Test to validate customer tier logic consistency
-- Ensures VIP customers have spent > 10000 and Preferred > 5000
SELECT 
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_analysis') }}
WHERE 
    (customer_tier = 'VIP' AND total_spent <= 10000)
    OR (customer_tier = 'Preferred' AND (total_spent <= 5000 OR total_spent > 10000))
    OR (customer_tier = 'Standard' AND total_spent > 5000)