-- Test: Data lineage integrity check
-- Validates that aggregated data in marts matches source totals

WITH source_totals AS (
    SELECT 
        COUNT(DISTINCT customer_id) AS total_customers,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity * price) AS total_revenue
    FROM {{ ref('stg_orders') }}
),
mart_totals AS (
    SELECT 
        COUNT(customer_id) AS total_customers,
        SUM(total_orders) AS total_orders,
        SUM(total_spent) AS total_revenue
    FROM {{ ref('customer_sales_analysis') }}
)

SELECT 
    'Customer count mismatch' AS test_type,
    s.total_customers AS source_value,
    m.total_customers AS mart_value
FROM source_totals s, mart_totals m
WHERE s.total_customers != m.total_customers

UNION ALL

SELECT 
    'Order count mismatch' AS test_type,
    s.total_orders AS source_value,
    m.total_orders AS mart_value
FROM source_totals s, mart_totals m
WHERE s.total_orders != m.total_orders

UNION ALL

SELECT 
    'Revenue mismatch' AS test_type,
    s.total_revenue AS source_value,
    m.total_revenue AS mart_value
FROM source_totals s, mart_totals m
WHERE ABS(s.total_revenue - m.total_revenue) > 0.01