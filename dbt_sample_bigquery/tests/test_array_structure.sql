-- Test to validate array structure and content
-- Ensures the last_3_orders array contains valid data

select
    customer_id,
    total_orders,
    array_size(last_3_orders) as array_length
from {{ ref('sales_analysis') }}
where 
    array_size(last_3_orders) > least(total_orders, 3)
    or array_size(last_3_orders) = 0