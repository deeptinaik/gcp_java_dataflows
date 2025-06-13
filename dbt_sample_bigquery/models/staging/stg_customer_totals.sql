{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for customer totals calculation
-- Calculates total spending and order count per customer
-- Converts BigQuery UNNEST to Snowflake LATERAL FLATTEN

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

customer_totals as (
    select
        customer_id,
        sum(quantity * price) as total_spent,
        count(distinct order_id) as total_orders,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from flattened_items
    group by customer_id
)

select * from customer_totals