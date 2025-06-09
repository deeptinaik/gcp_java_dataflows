-- Custom test to validate customer tier logic
-- Ensures that VIP customers have total_spent > 10000
-- Preferred customers have total_spent between 5001-10000
-- Standard customers have total_spent <= 5000

SELECT 
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_analytics') }}
WHERE 
    (customer_tier = 'VIP' AND total_spent <= 10000)
    OR (customer_tier = 'Preferred' AND (total_spent <= 5000 OR total_spent > 10000))
    OR (customer_tier = 'Standard' AND total_spent > 5000)