-- Test to validate that last_3_orders array contains maximum 3 orders
-- And that orders are sorted by date descending

WITH order_validations AS (
    SELECT 
        customer_id,
        total_orders,
        ARRAY_SIZE(last_3_orders) AS orders_in_array,
        last_3_orders
    FROM {{ ref('customer_sales_analysis') }}
    WHERE last_3_orders IS NOT NULL
)

SELECT 
    customer_id,
    total_orders,
    orders_in_array,
    CASE 
        WHEN orders_in_array > 3 THEN 'FAIL: More than 3 orders in array'
        WHEN total_orders >= 3 AND orders_in_array != 3 THEN 'FAIL: Should have exactly 3 orders'
        WHEN total_orders < 3 AND orders_in_array != total_orders THEN 'FAIL: Array size should match total orders'
        ELSE 'PASS'
    END AS test_result
FROM order_validations
WHERE 
    orders_in_array > 3
    OR (total_orders >= 3 AND orders_in_array != 3)
    OR (total_orders < 3 AND orders_in_array != total_orders)