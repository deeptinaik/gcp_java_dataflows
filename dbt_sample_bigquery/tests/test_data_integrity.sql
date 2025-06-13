-- Test to validate data integrity between staging and marts
-- Ensures no data loss during transformations

with staging_summary as (
    select
        count(distinct customer_id) as unique_customers,
        count(distinct order_id) as unique_orders,
        sum(quantity * price) as total_sales_amount
    from {{ ref('stg_orders') }}
),

marts_summary as (
    select
        count(distinct customer_id) as unique_customers,
        sum(total_orders) as total_orders,
        sum(total_spent) as total_sales_amount
    from {{ ref('sales_analysis') }}
),

comparison as (
    select
        s.unique_customers as staging_customers,
        m.unique_customers as marts_customers,
        s.unique_orders as staging_orders,
        m.total_orders as marts_orders,
        s.total_sales_amount as staging_amount,
        m.total_sales_amount as marts_amount
    from staging_summary s
    cross join marts_summary m
)

select *
from comparison
where 
    staging_customers != marts_customers
    or staging_orders != marts_orders
    or abs(staging_amount - marts_amount) > 0.01