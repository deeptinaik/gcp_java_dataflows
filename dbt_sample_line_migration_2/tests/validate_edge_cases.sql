-- Test for edge case: customers with null or negative amounts
-- Validates data quality and business logic constraints
SELECT 
    customer_id,
    total_spent,
    total_orders
FROM {{ ref('customer_analytics') }}
WHERE 
    total_spent IS NULL 
    OR total_spent < 0
    OR total_orders IS NULL
    OR total_orders <= 0