{{
  config(
    materialized='table',
    description='Purchase events with customer tier assignment and time windows'
  )
}}

-- Equivalent to EcommercePipeline.java processing:
-- 1. Filter successful purchases
-- 2. Add customer tier  
-- 3. Create time windows

SELECT
    customer_id,
    amount,
    status,
    event_timestamp,
    
    -- Add customer tier using macro (equivalent to AddCustomerTierFn)
    {{ assign_customer_tier('amount') }} AS customer_tier,
    
    -- Create time windows for aggregation (equivalent to FixedWindows)
    {{ create_time_windows('event_timestamp', var('window_minutes')) }} AS window_start,
    
    _loaded_at

FROM {{ ref('stg_purchase_events') }}
WHERE status = 'SUCCESS'  -- Filter only successful purchases