-- Test to ensure customer tier logic is correctly applied
SELECT 
    customer_id,
    total_spent,
    customer_tier,
    CASE 
        WHEN total_spent > 10000 AND customer_tier != 'VIP' THEN 1
        WHEN total_spent > 5000 AND total_spent <= 10000 AND customer_tier != 'Preferred' THEN 1
        WHEN total_spent <= 5000 AND customer_tier != 'Standard' THEN 1
        ELSE 0
    END AS tier_mismatch
FROM {{ ref('sales_analytics') }}
WHERE tier_mismatch = 1