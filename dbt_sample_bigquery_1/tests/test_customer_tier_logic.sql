/*
  Test to validate that customer tier logic is applied correctly
  VIP customers should have total_spent > 10000
  Preferred customers should have total_spent between 5000 and 10000
  Standard customers should have total_spent < 5000
*/

SELECT
    customer_id,
    total_spent,
    customer_tier
FROM {{ ref('customer_analysis') }}
WHERE
    (customer_tier = 'VIP' AND total_spent <= {{ var('vip_threshold') }})
    OR
    (customer_tier = 'Preferred' AND (total_spent <= {{ var('preferred_threshold') }} OR total_spent > {{ var('vip_threshold') }}))
    OR
    (customer_tier = 'Standard' AND total_spent >= {{ var('preferred_threshold') }})