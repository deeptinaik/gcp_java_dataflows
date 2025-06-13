-- Test to ensure total_spent calculation matches individual order totals
-- Validates aggregation accuracy from source to final model
WITH order_totals AS (
    SELECT 
        customer_id,
        SUM(quantity * price) AS calculated_total
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT 
    ca.customer_id,
    ca.total_spent,
    ot.calculated_total,
    ABS(ca.total_spent - ot.calculated_total) AS difference
FROM {{ ref('customer_analysis') }} ca
JOIN order_totals ot ON ca.customer_id = ot.customer_id
WHERE ABS(ca.total_spent - ot.calculated_total) > 0.01