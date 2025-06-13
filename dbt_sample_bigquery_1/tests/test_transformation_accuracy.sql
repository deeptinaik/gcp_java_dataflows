/*
  Test to validate transformation accuracy
  Ensures calculated fields match expected business logic
*/

WITH validation_checks AS (
    SELECT
        customer_id,
        total_spent,
        total_orders,
        avg_order_value,
        -- Validate avg_order_value calculation
        CASE 
            WHEN ABS(avg_order_value - (total_spent / NULLIF(total_orders, 0))) > 0.01 
            THEN 1 
            ELSE 0 
        END AS avg_order_value_mismatch,
        -- Validate customer tier assignment
        CASE
            WHEN (total_spent > {{ var('vip_threshold') }} AND customer_tier != 'VIP')
                OR (total_spent > {{ var('preferred_threshold') }} AND total_spent <= {{ var('vip_threshold') }} AND customer_tier != 'Preferred')
                OR (total_spent <= {{ var('preferred_threshold') }} AND customer_tier != 'Standard')
            THEN 1
            ELSE 0
        END AS tier_assignment_error
    FROM {{ ref('customer_analysis') }}
)

SELECT *
FROM validation_checks
WHERE avg_order_value_mismatch = 1 
   OR tier_assignment_error = 1