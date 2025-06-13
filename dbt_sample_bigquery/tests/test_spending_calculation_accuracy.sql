-- Test that total spending calculations are accurate
-- Validates that customer totals match the sum of individual order totals

WITH customer_order_sums AS (
    SELECT 
        customer_id,
        SUM(order_total) AS calculated_total
    FROM {{ ref('int_ranked_orders') }}
    GROUP BY customer_id
)

SELECT 
    ca.customer_id,
    ca.total_spent AS analytics_total,
    cos.calculated_total,
    ABS(ca.total_spent - cos.calculated_total) AS difference
FROM {{ ref('customer_analytics') }} ca
JOIN customer_order_sums cos 
    ON ca.customer_id = cos.customer_id
WHERE ABS(ca.total_spent - cos.calculated_total) > 0.01  -- Allow for minor rounding differences