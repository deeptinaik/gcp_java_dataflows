-- Test: Validate data lineage integrity between source and final output
-- Ensures no customer data is lost or duplicated in the transformation pipeline

WITH source_summary AS (
    SELECT
        COUNT(DISTINCT customer_id) AS source_customer_count,
        COUNT(DISTINCT order_id) AS source_order_count,
        SUM(quantity * price) AS source_total_revenue
    FROM {{ source('raw_data', 'orders') }}
    WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
),

final_summary AS (
    SELECT
        COUNT(DISTINCT customer_id) AS final_customer_count,
        SUM(total_orders) AS final_order_count,
        SUM(total_spent) AS final_total_revenue
    FROM {{ ref('customer_analysis') }}
),

validation AS (
    SELECT
        s.source_customer_count,
        f.final_customer_count,
        s.source_order_count,
        f.final_order_count,
        s.source_total_revenue,
        f.final_total_revenue,
        -- Check for discrepancies
        CASE 
            WHEN s.source_customer_count != f.final_customer_count THEN 'CUSTOMER_COUNT_MISMATCH'
            WHEN s.source_order_count != f.final_order_count THEN 'ORDER_COUNT_MISMATCH'
            WHEN ABS(s.source_total_revenue - f.final_total_revenue) > 0.01 THEN 'REVENUE_MISMATCH'
            ELSE 'VALID'
        END AS validation_status
    FROM source_summary s
    CROSS JOIN final_summary f
)

-- This test should return no rows if data lineage is intact
SELECT *
FROM validation
WHERE validation_status != 'VALID'