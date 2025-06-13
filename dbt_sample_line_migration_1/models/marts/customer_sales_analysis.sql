{{
  config(
    materialized='table',
    schema='analytics_layer'
  )
}}

-- Final mart model combining customer analysis with recent orders
-- Replicates the original BigQuery business logic with Snowflake optimizations
-- Provides customer segmentation and recent order history

with customer_totals as (
    select * from {{ ref('stg_customer_totals') }}
),

ranked_orders as (
    select * from {{ ref('stg_ranked_orders') }}
    where order_rank <= 3  -- Filter to top 3 recent orders
),

recent_orders_aggregated as (
    select
        customer_id,
        array_agg(
            object_construct(
                'order_id', order_id,
                'order_total', order_total,
                'order_date', order_date
            )
        ) within group (order by order_date desc) as last_3_orders
    from ranked_orders
    group by customer_id
),

final_customer_analysis as (
    select
        c.customer_id,
        c.total_spent,
        c.total_orders,
        c.total_items,
        c.avg_item_value,
        c.first_order_date,
        c.last_order_date,
        
        -- Apply customer tier classification using macro
        {{ customer_tier_classification('c.total_spent') }} as customer_tier,
        
        -- Include recent orders array (equivalent to BigQuery ARRAY_AGG with LIMIT)
        coalesce(r.last_3_orders, array_construct()) as last_3_orders,
        
        -- Additional analytics fields
        datediff('day', c.first_order_date, c.last_order_date) as customer_lifetime_days,
        c.total_spent / nullif(c.total_orders, 0) as avg_order_value,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from customer_totals c
    left join recent_orders_aggregated r
        on c.customer_id = r.customer_id
)

select * from final_customer_analysis
order by total_spent desc