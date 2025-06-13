-- Test to validate last_3_orders array structure and content
-- Ensures each customer has at most 3 orders and they are properly structured

with order_array_validation as (
    select
        customer_id,
        last_3_orders,
        array_size(last_3_orders) as order_count,
        case 
            when array_size(last_3_orders) > {{ var('max_recent_orders') }} then 'TOO_MANY_ORDERS'
            when array_size(last_3_orders) = 0 then 'NO_ORDERS'
            else 'VALID'
        end as validation_status
    from {{ ref('customer_analysis') }}
)

select *
from order_array_validation
where validation_status != 'VALID'