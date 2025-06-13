-- Test to validate total_spent calculation is accurate
SELECT 
    customer_id,
    total_spent,
    SUM(order_data.value:order_total::NUMBER) as calculated_total
FROM {{ ref('customer_analysis') }},
     LATERAL FLATTEN(input => last_3_orders) order_data
GROUP BY customer_id, total_spent
HAVING total_spent != calculated_total