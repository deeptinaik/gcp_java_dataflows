-- Test to validate data lineage integrity across staging and mart models
-- Ensures customer totals match between staging and final mart

with staging_totals as (
    select
        customer_id,
        total_spent,
        total_orders,
        total_items
    from {{ ref('stg_customer_totals') }}
),

mart_totals as (
    select
        customer_id,
        total_spent,
        total_orders,
        total_items
    from {{ ref('customer_sales_analysis') }}
),

comparison as (
    select
        s.customer_id,
        s.total_spent as staging_spent,
        m.total_spent as mart_spent,
        s.total_orders as staging_orders,
        m.total_orders as mart_orders,
        s.total_items as staging_items,
        m.total_items as mart_items
    from staging_totals s
    full outer join mart_totals m
        on s.customer_id = m.customer_id
    where
        s.total_spent != m.total_spent
        or s.total_orders != m.total_orders
        or s.total_items != m.total_items
        or s.customer_id is null
        or m.customer_id is null
)

select * from comparison