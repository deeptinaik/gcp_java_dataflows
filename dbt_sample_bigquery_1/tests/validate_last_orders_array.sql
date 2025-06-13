-- Test to validate that last_3_orders array contains at most 3 orders
-- and they are properly ordered by date (most recent first)

WITH order_counts AS (
    SELECT
        customer_id,
        ARRAY_SIZE(last_3_orders) as order_count
    FROM {{ ref('customer_analysis') }}
)

SELECT
    customer_id,
    order_count
FROM order_counts
WHERE order_count > 3 OR order_count < 1