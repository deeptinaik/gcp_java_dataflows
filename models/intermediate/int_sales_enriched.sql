{{
  config(
    materialized='table',
    description='Sales data enriched with product information and store details'
  )
}}

-- Equivalent to RetailDataflowJava SalesDataPipeline:
-- 1. Parse sales data
-- 2. Enrich with product info
-- 3. Format for output

SELECT
    sale_id,
    product_id,
    store_id,
    quantity,
    total_amount,
    sale_timestamp,
    
    -- Enrichment equivalent to ProductTransform (dummy enrichment for now)
    store_id || '_enriched' AS enriched_store_id,
    
    -- Calculate derived fields
    total_amount / quantity AS unit_price,
    
    -- Add categorization
    CASE
      WHEN total_amount > 1000 THEN 'HIGH_VALUE'
      WHEN total_amount > 100 THEN 'MEDIUM_VALUE'
      ELSE 'LOW_VALUE'
    END AS sale_category,
    
    _loaded_at

FROM {{ ref('stg_sales_data') }}