-- Test to validate customer tier assignment logic
-- Ensures that customer tiers are correctly assigned based on spending thresholds

with tier_validation as (
    select
        customer_id,
        total_spent,
        customer_tier,
        case
            when total_spent > {{ var('vip_threshold') }} then 'VIP'
            when total_spent > {{ var('preferred_threshold') }} then 'Preferred'
            else 'Standard'
        end as expected_tier
    from {{ ref('customer_analysis') }}
)

select *
from tier_validation
where customer_tier != expected_tier