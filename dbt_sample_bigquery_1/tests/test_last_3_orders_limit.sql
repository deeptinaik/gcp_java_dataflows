-- Test that last_3_orders array contains at most 3 orders
SELECT
    customer_id,
    ARRAY_SIZE(last_3_orders) AS order_count
FROM {{ ref('mart_customer_analysis') }}
WHERE ARRAY_SIZE(last_3_orders) > 3