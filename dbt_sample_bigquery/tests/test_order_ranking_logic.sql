-- Test that order ranking is working correctly
-- Validates that each customer has properly ranked orders by date

SELECT customer_id, order_date, order_rank
FROM {{ ref('int_ranked_orders') }}
WHERE order_rank = 1
    AND order_date != (
        SELECT MAX(order_date)
        FROM {{ ref('int_ranked_orders') }} inner_ranks
        WHERE inner_ranks.customer_id = int_ranked_orders.customer_id
    )