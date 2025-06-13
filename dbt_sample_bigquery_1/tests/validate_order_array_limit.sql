-- Test to validate that last_3_orders array contains at most 3 orders
SELECT 
    customer_id,
    ARRAY_SIZE(last_3_orders) as order_count
FROM {{ ref('customer_analysis') }}
WHERE ARRAY_SIZE(last_3_orders) > {{ var('max_recent_orders') }}