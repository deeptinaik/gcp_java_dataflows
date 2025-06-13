-- Test: Validate data consistency between aggregations
-- Ensures that customer totals are correctly calculated from underlying order data
WITH source_totals AS (
    SELECT 
        customer_id,
        SUM(quantity * price) AS source_total_spent,
        COUNT(DISTINCT order_id) AS source_total_orders
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),
analysis_totals AS (
    SELECT
        customer_id,
        total_spent,
        total_orders
    FROM {{ ref('customer_analysis') }}
)
SELECT 
    s.customer_id,
    s.source_total_spent,
    a.total_spent,
    s.source_total_orders,
    a.total_orders,
    ABS(s.source_total_spent - a.total_spent) AS spending_difference,
    ABS(s.source_total_orders - a.total_orders) AS order_count_difference
FROM source_totals s
JOIN analysis_totals a ON s.customer_id = a.customer_id
WHERE 
    ABS(s.source_total_spent - a.total_spent) > 0.01
    OR s.source_total_orders != a.total_orders