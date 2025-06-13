-- Test for edge cases: null handling and boundary conditions
-- Validates that the models handle edge cases gracefully

SELECT *
FROM {{ ref('customer_analytics') }}
WHERE 
    -- Check for null values in critical fields
    customer_id IS NULL
    OR total_spent IS NULL
    OR total_orders IS NULL
    OR customer_tier IS NULL
    OR last_3_orders IS NULL
    -- Check for invalid boundary conditions
    OR total_spent < 0
    OR total_orders < 1
    OR ARRAY_SIZE(last_3_orders) = 0