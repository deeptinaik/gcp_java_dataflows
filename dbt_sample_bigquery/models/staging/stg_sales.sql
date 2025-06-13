{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for sales aggregation
-- Aggregates order items into structured arrays per order
-- Converts BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake equivalent

with order_items as (
    select
        order_id,
        customer_id,
        order_date,
        product_id,
        quantity,
        price
    from {{ ref('stg_orders') }}
),

sales_aggregated as (
    select
        order_id,
        customer_id,
        order_date,
        -- Convert BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake array of objects
        array_agg(object_construct(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )) as items,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from order_items
    group by
        order_id, customer_id, order_date
)

select * from sales_aggregated