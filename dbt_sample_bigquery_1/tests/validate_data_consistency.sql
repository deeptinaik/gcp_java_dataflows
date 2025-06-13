-- Test to ensure data consistency between staging and marts layers
-- Validates that customer totals match between stg_customer_totals and customer_analysis

SELECT
    ct.customer_id,
    ct.total_spent as staging_total_spent,
    ca.total_spent as marts_total_spent,
    ct.total_orders as staging_total_orders,
    ca.total_orders as marts_total_orders
FROM {{ ref('stg_customer_totals') }} ct
JOIN {{ ref('customer_analysis') }} ca
ON ct.customer_id = ca.customer_id
WHERE 
    ct.total_spent != ca.total_spent 
    OR ct.total_orders != ca.total_orders