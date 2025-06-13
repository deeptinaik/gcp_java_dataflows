{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for orders data
-- Clean and standardize the source orders data

with source_data as (
    select * from {{ source('sales_raw', 'orders') }}
),

standardized as (
    select
        order_id,
        customer_id,
        {{ safe_cast('order_date', 'DATE') }} as order_date,
        product_id,
        {{ safe_cast('quantity', 'INTEGER') }} as quantity,
        {{ safe_cast('price', 'DECIMAL(10,2)') }} as price,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from source_data
    where order_id is not null
      and customer_id is not null
      and order_date is not null
      and product_id is not null
      and quantity > 0
      and price >= 0
)

select * from standardized