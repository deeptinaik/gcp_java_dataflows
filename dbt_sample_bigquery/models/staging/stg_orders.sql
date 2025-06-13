{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for orders data
-- Prepares and validates source data for downstream transformations

with source_data as (
    select * from {{ source('raw_sales', 'orders') }}
),

-- Validate and prepare the data
prepared as (
    select
        order_id,
        customer_id,
        order_date,
        product_id,
        {{ safe_cast('quantity', 'INTEGER') }} as quantity,
        {{ safe_cast('price', 'NUMERIC') }} as price,
        
        -- Add audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from source_data
    where 
        order_id is not null
        and customer_id is not null
        and order_date is not null
        and quantity > 0
        and price >= 0
)

select * from prepared