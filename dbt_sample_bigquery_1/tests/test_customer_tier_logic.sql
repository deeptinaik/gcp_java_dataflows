-- Test to validate customer tier classification logic
-- Ensures VIP customers have total_spent > 10000
-- Ensures Preferred customers have total_spent between 5000 and 10000
-- Ensures Standard customers have total_spent < 5000

SELECT
    customer_id,
    total_spent,
    customer_tier,
    CASE
        WHEN customer_tier = 'VIP' AND total_spent <= {{ var('vip_threshold') }} THEN 'VIP_TIER_ERROR'
        WHEN customer_tier = 'Preferred' AND (total_spent <= {{ var('preferred_threshold') }} OR total_spent > {{ var('vip_threshold') }}) THEN 'PREFERRED_TIER_ERROR'
        WHEN customer_tier = 'Standard' AND total_spent >= {{ var('preferred_threshold') }} THEN 'STANDARD_TIER_ERROR'
        ELSE 'CORRECT'
    END AS tier_validation_result
FROM {{ ref('customer_analysis') }}
WHERE CASE
    WHEN customer_tier = 'VIP' AND total_spent <= {{ var('vip_threshold') }} THEN 1
    WHEN customer_tier = 'Preferred' AND (total_spent <= {{ var('preferred_threshold') }} OR total_spent > {{ var('vip_threshold') }}) THEN 1
    WHEN customer_tier = 'Standard' AND total_spent >= {{ var('preferred_threshold') }} THEN 1
    ELSE 0
END = 1