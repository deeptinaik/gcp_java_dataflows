-- Test to validate data lineage integrity
-- Ensures that the number of customers in the final model matches the staging models

with staging_customers as (
    select count(distinct customer_id) as staging_customer_count
    from {{ ref('stg_customer_totals') }}
),

final_customers as (
    select count(distinct customer_id) as final_customer_count
    from {{ ref('customer_analysis') }}
)

select 
    s.staging_customer_count,
    f.final_customer_count,
    s.staging_customer_count - f.final_customer_count as difference
from staging_customers s
cross join final_customers f
where s.staging_customer_count != f.final_customer_count