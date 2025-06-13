-- Test to validate order aggregation accuracy
WITH order_validation AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        SUM((items.value:quantity)::NUMBER * (items.value:price)::NUMBER) AS calculated_total
    FROM {{ ref('stg_orders') }},
         LATERAL FLATTEN(input => items) AS items
    GROUP BY order_id, customer_id, order_date
),
ranked_validation AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_total
    FROM {{ ref('int_ranked_orders') }}
)
SELECT 
    ov.order_id,
    ov.calculated_total,
    rv.order_total,
    ABS(ov.calculated_total - rv.order_total) AS difference
FROM order_validation ov
JOIN ranked_validation rv ON ov.order_id = rv.order_id
WHERE ABS(ov.calculated_total - rv.order_total) > 0.01