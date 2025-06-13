-- Test to validate that last_3_orders array contains correct number of orders
SELECT 
    customer_id,
    total_orders,
    ARRAY_SIZE(last_3_orders) AS last_orders_count
FROM {{ ref('customer_sales_analysis') }}
WHERE 
    (total_orders >= 3 AND ARRAY_SIZE(last_3_orders) != 3)
    OR (total_orders < 3 AND ARRAY_SIZE(last_3_orders) != total_orders)