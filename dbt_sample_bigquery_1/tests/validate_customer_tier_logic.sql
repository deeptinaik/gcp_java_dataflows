-- Test: Validate customer tier calculation logic
-- Ensures that customers are correctly categorized based on spending thresholds
SELECT 
    customer_id,
    total_spent,
    customer_tier,
    CASE
        WHEN total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP' THEN 'VIP_MISMATCH'
        WHEN total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred' THEN 'PREFERRED_MISMATCH'
        WHEN total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard' THEN 'STANDARD_MISMATCH'
        ELSE NULL
    END AS tier_validation_error
FROM {{ ref('customer_analysis') }}
WHERE 
    CASE
        WHEN total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP' THEN TRUE
        WHEN total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred' THEN TRUE
        WHEN total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard' THEN TRUE
        ELSE FALSE
    END