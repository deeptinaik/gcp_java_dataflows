-- Test to validate data consistency between total_spent calculation and order totals
-- Ensures aggregations are consistent across different models
WITH order_totals AS (
    SELECT 
        customer_id,
        SUM(quantity * price) AS calculated_total_spent
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT 
    ca.customer_id,
    ca.total_spent,
    ot.calculated_total_spent,
    ABS(ca.total_spent - ot.calculated_total_spent) AS difference
FROM {{ ref('customer_analytics') }} ca
JOIN order_totals ot ON ca.customer_id = ot.customer_id
WHERE ABS(ca.total_spent - ot.calculated_total_spent) > 0.01