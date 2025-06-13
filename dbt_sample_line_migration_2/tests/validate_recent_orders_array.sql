-- Test to validate order array structure and content
-- Ensures last_3_orders array has correct structure and max 3 orders
SELECT 
    customer_id,
    total_orders,
    last_3_orders,
    ARRAY_SIZE(last_3_orders) AS array_size
FROM {{ ref('customer_analytics') }}
WHERE 
    last_3_orders IS NULL
    OR ARRAY_SIZE(last_3_orders) = 0
    OR ARRAY_SIZE(last_3_orders) > {{ var('max_recent_orders') }}
    OR (total_orders > 0 AND ARRAY_SIZE(last_3_orders) = 0)