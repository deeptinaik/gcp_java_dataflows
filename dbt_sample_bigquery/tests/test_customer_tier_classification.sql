-- Test that customer tier classification is working correctly
-- Validates the tier logic: VIP > 10000, Preferred > 5000, else Standard

SELECT *
FROM {{ ref('customer_analytics') }}
WHERE 
    (total_spent > 10000 AND customer_tier != 'VIP')
    OR (total_spent > 5000 AND total_spent <= 10000 AND customer_tier != 'Preferred')
    OR (total_spent <= 5000 AND customer_tier != 'Standard')