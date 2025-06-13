{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for sales data aggregation
-- Converts BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake object structure
-- Replaces UNNEST operations with proper Snowflake joins

with source_orders as (
    select * from {{ source('raw_sales', 'orders') }}
),

sales_aggregated as (
    select
        order_id,
        customer_id,
        order_date,
        -- Convert BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake equivalent
        array_agg(
            object_construct(
                'product_id', product_id,
                'quantity', quantity,
                'price', price
            )
        ) as items,
        
        -- Calculate order totals for downstream use
        sum(quantity * price) as order_total,
        count(*) as item_count,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from source_orders
    group by
        order_id, customer_id, order_date
)

select * from sales_aggregated