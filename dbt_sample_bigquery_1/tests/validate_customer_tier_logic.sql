-- Test to validate customer tier assignment logic
-- This test ensures VIP customers have spent more than 10,000
-- Preferred customers have spent between 5,000 and 10,000
-- Standard customers have spent less than 5,000

SELECT
    customer_id,
    total_spent,
    customer_tier,
    CASE
        WHEN total_spent > 10000 THEN 'VIP'
        WHEN total_spent > 5000 THEN 'Preferred'
        ELSE 'Standard'
    END AS expected_tier
FROM {{ ref('customer_analysis') }}
WHERE customer_tier != (
    CASE
        WHEN total_spent > 10000 THEN 'VIP'
        WHEN total_spent > 5000 THEN 'Preferred'
        ELSE 'Standard'
    END
)