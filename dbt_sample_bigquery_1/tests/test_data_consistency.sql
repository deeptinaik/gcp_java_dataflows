-- Test to validate data consistency between intermediate and final models
WITH source_totals AS (
    SELECT
        customer_id,
        SUM(quantity * price) AS calculated_total,
        COUNT(DISTINCT order_id) AS calculated_orders
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT 
    s.customer_id,
    s.calculated_total,
    c.total_spent,
    s.calculated_orders,
    c.total_orders
FROM source_totals s
JOIN {{ ref('customer_sales_analysis') }} c
    ON s.customer_id = c.customer_id
WHERE 
    ABS(s.calculated_total - c.total_spent) > 0.01
    OR s.calculated_orders != c.total_orders