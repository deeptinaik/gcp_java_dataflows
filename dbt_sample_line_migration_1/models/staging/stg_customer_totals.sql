{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for customer totals calculation
-- Replaces BigQuery UNNEST operations with standard joins
-- Calculates total spending and order counts per customer

with source_orders as (
    select * from {{ source('raw_sales', 'orders') }}
),

customer_totals_calculated as (
    select
        customer_id,
        sum(quantity * price) as total_spent,
        count(distinct order_id) as total_orders,
        count(*) as total_items,
        avg(quantity * price) as avg_item_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from source_orders
    group by customer_id
)

select * from customer_totals_calculated