-- Test: Validate edge cases with zero and null values
-- Ensures system handles edge cases gracefully

SELECT
    customer_id,
    total_spent,
    total_orders,
    customer_tier,
    spending_per_order
FROM {{ ref('customer_analysis') }}
WHERE 
    total_spent IS NULL
    OR total_orders IS NULL
    OR total_orders = 0
    OR customer_tier IS NULL
    OR (total_orders > 0 AND spending_per_order IS NULL)
    OR (total_spent = 0 AND customer_tier != 'Standard')