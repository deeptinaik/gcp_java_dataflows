-- Test to validate customer tier logic matches thresholds
SELECT 
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_analysis') }}
WHERE 
    (total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP') OR
    (total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred') OR
    (total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard')