-- Test to validate data consistency between intermediate and final models
-- Ensures customer totals match between int_customer_totals and customer_analysis

SELECT
    ca.customer_id,
    ca.total_spent AS final_total_spent,
    ct.total_spent AS intermediate_total_spent,
    ca.total_orders AS final_total_orders,
    ct.total_orders AS intermediate_total_orders,
    ABS(ca.total_spent - ct.total_spent) AS spent_difference,
    ABS(ca.total_orders - ct.total_orders) AS orders_difference
FROM {{ ref('customer_analysis') }} ca
JOIN {{ ref('int_customer_totals') }} ct
    ON ca.customer_id = ct.customer_id
WHERE ABS(ca.total_spent - ct.total_spent) > 0.01
   OR ABS(ca.total_orders - ct.total_orders) > 0