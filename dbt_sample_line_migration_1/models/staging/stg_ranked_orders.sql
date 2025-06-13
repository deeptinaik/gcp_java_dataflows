{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for ranked orders calculation
-- Preserves window function logic for order ranking
-- Calculates order totals and ranks by recency

with source_orders as (
    select * from {{ source('raw_sales', 'orders') }}
),

order_totals as (
    select
        order_id,
        customer_id,
        order_date,
        sum(quantity * price) as order_total,
        count(*) as item_count
    from source_orders
    group by order_id, customer_id, order_date
),

ranked_orders_calculated as (
    select
        order_id,
        customer_id,
        order_date,
        order_total,
        item_count,
        
        -- Window function for ranking orders by recency
        rank() over (
            partition by customer_id 
            order by order_date desc
        ) as order_rank,
        
        row_number() over (
            partition by customer_id 
            order by order_date desc
        ) as order_row_num,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from order_totals
)

select * from ranked_orders_calculated