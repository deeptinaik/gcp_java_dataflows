-- Test to validate that calculated totals match between staging and marts
-- Ensures data integrity across model transformations

SELECT 
    s.customer_id,
    s.total_spent AS staging_total_spent,
    m.total_spent AS marts_total_spent,
    s.total_orders AS staging_total_orders,
    m.total_orders AS marts_total_orders,
    ABS(s.total_spent - m.total_spent) AS spent_difference,
    ABS(s.total_orders - m.total_orders) AS orders_difference
FROM {{ ref('stg_customer_totals') }} s
JOIN {{ ref('customer_sales_analysis') }} m
ON s.customer_id = m.customer_id
WHERE 
    ABS(s.total_spent - m.total_spent) > 0.01  -- Allow for minor rounding differences
    OR ABS(s.total_orders - m.total_orders) > 0