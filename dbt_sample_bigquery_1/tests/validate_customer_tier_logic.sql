-- Test: Validate customer tier logic
-- Ensures that VIP customers have spent more than preferred threshold
-- and preferred customers have spent more than standard threshold

SELECT 
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_sales_analysis') }}
WHERE 
    (customer_tier = 'VIP' AND total_spent <= {{ var('vip_threshold') }})
    OR (customer_tier = 'Preferred' AND total_spent <= {{ var('preferred_threshold') }})
    OR (customer_tier = 'Standard' AND total_spent > {{ var('preferred_threshold') }})