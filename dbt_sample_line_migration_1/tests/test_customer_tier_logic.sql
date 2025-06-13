-- Test to validate customer tier logic matches business rules
-- Ensures VIP customers have spending > 10000
-- Ensures Preferred customers have spending between 5000-10000
-- Ensures Standard customers have spending <= 5000

select
    customer_id,
    total_spent,
    customer_tier,
    case
        when customer_tier = 'VIP' and total_spent <= 10000 then 'FAIL: VIP should have > 10000'
        when customer_tier = 'Preferred' and (total_spent <= 5000 or total_spent > 10000) then 'FAIL: Preferred should be 5000-10000'
        when customer_tier = 'Standard' and total_spent > 5000 then 'FAIL: Standard should be <= 5000'
        else 'PASS'
    end as test_result
from {{ ref('customer_sales_analysis') }}
where case
    when customer_tier = 'VIP' and total_spent <= 10000 then 'FAIL'
    when customer_tier = 'Preferred' and (total_spent <= 5000 or total_spent > 10000) then 'FAIL'
    when customer_tier = 'Standard' and total_spent > 5000 then 'FAIL'
    else 'PASS'
end = 'FAIL'