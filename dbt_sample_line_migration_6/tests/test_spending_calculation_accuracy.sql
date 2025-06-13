-- Test: Validate spending calculation accuracy
-- Ensures total_spent equals sum of all order totals for each customer

WITH customer_order_sums AS (
    SELECT
        customer_id,
        SUM(order_total) AS calculated_total_spent
    FROM {{ ref('stg_ranked_orders') }}
    GROUP BY customer_id
)

SELECT
    ca.customer_id,
    ca.total_spent,
    cos.calculated_total_spent,
    ABS(ca.total_spent - cos.calculated_total_spent) AS difference
FROM {{ ref('customer_analysis') }} ca
JOIN customer_order_sums cos 
    ON ca.customer_id = cos.customer_id
WHERE ABS(ca.total_spent - cos.calculated_total_spent) > 0.01