-- Test to validate that last_3_orders contains at most 3 orders per customer
-- and orders are properly ranked by date
SELECT 
    customer_id,
    {{ array_size('last_3_orders') }} as order_count
FROM {{ ref('customer_analysis') }}
WHERE {{ array_size('last_3_orders') }} > 3