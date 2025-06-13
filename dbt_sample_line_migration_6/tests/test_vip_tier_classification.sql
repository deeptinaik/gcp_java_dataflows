-- Test: Validate VIP tier classification logic
-- Ensures customers with spending > 10000 are classified as VIP

SELECT
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_analysis') }}
WHERE 
    (total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP')
    OR 
    (total_spent <= {{ var('vip_threshold') }} AND customer_tier = 'VIP')