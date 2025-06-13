-- Test: Validate recent orders JSON structure and content
-- Ensures the ARRAY_AGG replacement works correctly

WITH order_validation AS (
    SELECT
        customer_id,
        last_3_orders,
        -- Parse and validate JSON structure
        ARRAY_SIZE(last_3_orders) AS orders_count,
        -- Check that we have at most 3 orders
        CASE 
            WHEN ARRAY_SIZE(last_3_orders) > 3 THEN 'TOO_MANY_ORDERS'
            WHEN ARRAY_SIZE(last_3_orders) = 0 THEN 'NO_ORDERS'
            ELSE 'VALID_COUNT'
        END AS count_validation,
        -- Validate JSON structure for first order (if exists)
        CASE
            WHEN ARRAY_SIZE(last_3_orders) > 0 THEN
                CASE
                    WHEN last_3_orders[0]:order_id IS NULL THEN 'MISSING_ORDER_ID'
                    WHEN last_3_orders[0]:order_total IS NULL THEN 'MISSING_ORDER_TOTAL'
                    WHEN last_3_orders[0]:order_date IS NULL THEN 'MISSING_ORDER_DATE'
                    ELSE 'VALID_STRUCTURE'
                END
            ELSE 'VALID_STRUCTURE'
        END AS structure_validation
    FROM {{ ref('customer_analysis') }}
)

-- This test should return no rows if JSON structure is correct
SELECT *
FROM order_validation
WHERE count_validation != 'VALID_COUNT' 
   OR structure_validation != 'VALID_STRUCTURE'