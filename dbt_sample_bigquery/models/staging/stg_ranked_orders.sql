{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for ranked orders calculation
-- Ranks orders by date for each customer and calculates order totals
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN with window functions

with sales_data as (
    select
        order_id,
        customer_id,
        order_date,
        items
    from {{ ref('stg_sales') }}
),

-- Flatten the items array to individual rows (equivalent to UNNEST in BigQuery)
flattened_items as (
    select
        s.order_id,
        s.customer_id,
        s.order_date,
        f.value:product_id::string as product_id,
        f.value:quantity::integer as quantity,
        f.value:price::decimal(10,2) as price
    from sales_data s,
    lateral flatten(input => s.items) f
),

-- Calculate order totals and rank by date
ranked_orders as (
    select
        order_id,
        customer_id,
        order_date,
        sum(quantity * price) as order_total,
        rank() over (partition by customer_id order by order_date desc) as order_rank,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from flattened_items
    group by
        order_id, customer_id, order_date
)

select * from ranked_orders