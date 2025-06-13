{{
  config(
    materialized='table',
    schema='analytics_layer',
    pre_hook="TRUNCATE TABLE {{ this }}",
    cluster_by=['customer_tier'],
    partition_by={
      "field": "analysis_date",
      "data_type": "date"
    }
  )
}}

-- Main customer analysis model combining all staging components
-- Replicates the complete business logic from the original BigQuery SQL
-- Provides customer segmentation with recent order history

with customer_totals as (
    select * from {{ ref('stg_customer_totals') }}
),

ranked_orders as (
    select * from {{ ref('stg_ranked_orders') }}
    where order_rank <= {{ var('max_recent_orders') }}
),

customer_analysis as (
    select
        c.customer_id,
        c.total_spent,
        c.total_orders,
        
        -- Aggregate last 3 orders using Snowflake array aggregation
        array_agg(object_construct(
            'order_id', r.order_id,
            'order_total', r.order_total,
            'order_date', r.order_date
        )) within group (order by r.order_date desc) as last_3_orders,
        
        -- Customer tier classification based on total spending
        case
            when c.total_spent > {{ var('vip_threshold') }} then 'VIP'
            when c.total_spent > {{ var('preferred_threshold') }} then 'Preferred'
            else 'Standard'
        end as customer_tier,
        
        -- Analytics metadata
        current_date() as analysis_date,
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from customer_totals c
    join ranked_orders r
        on c.customer_id = r.customer_id
    group by
        c.customer_id, 
        c.total_spent, 
        c.total_orders
)

select * from customer_analysis
order by total_spent desc