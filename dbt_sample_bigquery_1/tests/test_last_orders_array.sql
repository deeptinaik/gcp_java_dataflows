-- Test to validate that last_3_orders array contains correct order data
-- Ensures the array structure and ordering is correct

SELECT
    customer_id,
    last_3_orders,
    ARRAY_SIZE(last_3_orders) AS order_count,
    'ARRAY_SIZE_ERROR' AS error_type
FROM {{ ref('customer_analysis') }}
WHERE ARRAY_SIZE(last_3_orders) > 3

UNION ALL

-- Test to ensure orders in last_3_orders are properly ordered by date (descending)
SELECT
    customer_id,
    last_3_orders,
    ARRAY_SIZE(last_3_orders) AS order_count,
    'ORDER_SEQUENCE_ERROR' AS error_type
FROM {{ ref('customer_analysis') }}
WHERE ARRAY_SIZE(last_3_orders) > 1
  AND (
    -- Check if dates are not in descending order
    last_3_orders[0]:order_date::DATE < last_3_orders[1]:order_date::DATE
    OR (ARRAY_SIZE(last_3_orders) > 2 AND last_3_orders[1]:order_date::DATE < last_3_orders[2]:order_date::DATE)
  )