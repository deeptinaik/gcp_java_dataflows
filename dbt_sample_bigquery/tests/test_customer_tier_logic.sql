-- Test: Validate customer tier logic matches original BigQuery rules
-- Ensures the macro produces correct tier classifications

SELECT
    customer_id,
    total_spent,
    customer_tier,
    -- Check that tier logic is correct
    CASE
        WHEN total_spent > 10000 AND customer_tier != 'VIP' THEN 'VIP_ERROR'
        WHEN total_spent > 5000 AND total_spent <= 10000 AND customer_tier != 'Preferred' THEN 'PREFERRED_ERROR'
        WHEN total_spent <= 5000 AND customer_tier != 'Standard' THEN 'STANDARD_ERROR'
        ELSE 'CORRECT'
    END AS tier_validation_status
FROM {{ ref('customer_analysis') }}
WHERE 
    -- This test should return no rows if tier logic is correct
    CASE
        WHEN total_spent > 10000 AND customer_tier != 'VIP' THEN 1
        WHEN total_spent > 5000 AND total_spent <= 10000 AND customer_tier != 'Preferred' THEN 1
        WHEN total_spent <= 5000 AND customer_tier != 'Standard' THEN 1
        ELSE 0
    END = 1