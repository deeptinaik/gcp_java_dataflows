-- Test that no order totals are negative
WITH order_totals AS (
    SELECT
        customer_id,
        f.value:order_total::NUMBER AS order_total
    FROM {{ ref('mart_customer_analysis') }},
    LATERAL FLATTEN(INPUT => last_3_orders) f
)
SELECT
    customer_id,
    order_total
FROM order_totals
WHERE order_total < 0