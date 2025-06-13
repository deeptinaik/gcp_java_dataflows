-- Test to validate customer tier classification logic
-- Ensures customer tiers are assigned correctly based on spending thresholds

select
    customer_id,
    total_spent,
    customer_tier,
    case
        when total_spent > 10000 then 'VIP'
        when total_spent > 5000 then 'Preferred'
        else 'Standard'
    end as expected_tier
from {{ ref('sales_analysis') }}
where customer_tier != case
    when total_spent > 10000 then 'VIP'
    when total_spent > 5000 then 'Preferred'
    else 'Standard'
end