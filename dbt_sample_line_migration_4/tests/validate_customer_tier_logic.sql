-- Test to validate customer tier classification logic
-- Ensures that VIP customers have spending > 10000, Preferred > 5000, etc.

SELECT 
    customer_id,
    total_spent,
    customer_tier,
    CASE 
        WHEN total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP' THEN 'FAIL: Should be VIP'
        WHEN total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred' THEN 'FAIL: Should be Preferred'
        WHEN total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard' THEN 'FAIL: Should be Standard'
        ELSE 'PASS'
    END AS test_result
FROM {{ ref('customer_sales_analysis') }}
WHERE 
    CASE 
        WHEN total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP' THEN TRUE
        WHEN total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred' THEN TRUE
        WHEN total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard' THEN TRUE
        ELSE FALSE
    END