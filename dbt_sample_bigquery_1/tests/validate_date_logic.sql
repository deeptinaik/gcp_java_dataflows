-- Test: Validate order date logic and edge cases
-- Ensures that first_order_date <= last_order_date and customer_lifetime_days is accurate
SELECT 
    customer_id,
    first_order_date,
    last_order_date,
    customer_lifetime_days,
    DATEDIFF('day', first_order_date, last_order_date) AS calculated_lifetime_days
FROM {{ ref('customer_analysis') }}
WHERE 
    first_order_date > last_order_date
    OR customer_lifetime_days != DATEDIFF('day', first_order_date, last_order_date)
    OR customer_lifetime_days < 0