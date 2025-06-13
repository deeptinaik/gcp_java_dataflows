{{
  config(
    materialized='table',
    schema='analytics_layer'
  )
}}

-- Sales analysis model replicating the original BigQuery logic
-- Analyzes sales data with advanced transformations including customer tier classification

with sales_grouped as (
  select
    order_id,
    customer_id,
    order_date,
    -- Convert BigQuery ARRAY_AGG(STRUCT()) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT())
    array_agg(
      object_construct(
        'product_id', product_id,
        'quantity', quantity,
        'price', price
      )
    ) as items
  from {{ ref('stg_orders') }}
  group by
    order_id, customer_id, order_date
),

-- Calculate customer totals using LATERAL FLATTEN instead of UNNEST
customer_totals as (
  select
    customer_id,
    sum(item_data.value:"quantity"::number * item_data.value:"price"::number) as total_spent,
    count(distinct order_id) as total_orders
  from sales_grouped,
       lateral flatten(input => sales_grouped.items) as item_data
  group by
    customer_id
),

-- Calculate ranked orders for each customer
ranked_orders as (
  select
    s.order_id,
    s.customer_id,
    s.order_date,
    sum(item_data.value:"quantity"::number * item_data.value:"price"::number) as order_total,
    rank() over (partition by s.customer_id order by s.order_date desc) as order_rank
  from sales_grouped s,
       lateral flatten(input => s.items) as item_data
  group by
    s.order_id, s.customer_id, s.order_date
),

-- Final result set
final_result as (
  select
    c.customer_id,
    c.total_spent,
    c.total_orders,
    -- Replicate ARRAY_AGG with ORDER BY and LIMIT functionality
    {{ snowflake_array_agg_struct('r.order_id', 'r.order_total', 'r.order_date') }}
      within group (order by r.order_date desc) as last_3_orders,
    -- Use macro for customer tier classification
    {{ customer_tier_classification('c.total_spent') }} as customer_tier,
    
    -- Add audit fields
    {{ generate_current_timestamp() }} as dbt_load_timestamp,
    {{ generate_current_timestamp() }} as dbt_update_timestamp
  from
    customer_totals c
  join
    ranked_orders r
  on
    c.customer_id = r.customer_id
  where
    r.order_rank <= {{ var('max_recent_orders') }}
  group by
    c.customer_id, c.total_spent, c.total_orders
)

select * from final_result
order by total_spent desc