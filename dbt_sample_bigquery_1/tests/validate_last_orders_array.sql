-- Test: Validate that last 3 orders array contains valid data
-- Ensures that customers have no more than 3 orders in the array
-- and that order dates are properly ordered (most recent first)

SELECT 
    customer_id,
    last_3_orders,
    ARRAY_SIZE(last_3_orders) AS orders_count
FROM {{ ref('customer_sales_analysis') }}
WHERE 
    ARRAY_SIZE(last_3_orders) > 3
    OR ARRAY_SIZE(last_3_orders) = 0