{{
  config(
    materialized='table',
    description='Retail sales analysis and reporting - equivalent to RetailDataflowJava output'
  )
}}

-- Equivalent to RetailDataflowJava pipeline output
-- Sales data with enrichment and analytics

SELECT
    sale_id,
    product_id,
    store_id,
    enriched_store_id,
    quantity,
    total_amount,
    unit_price,
    sale_category,
    sale_timestamp,
    
    -- Store-level aggregations
    SUM(total_amount) OVER (PARTITION BY store_id) AS store_total_sales,
    AVG(total_amount) OVER (PARTITION BY store_id) AS store_avg_sale_amount,
    COUNT(*) OVER (PARTITION BY store_id) AS store_transaction_count,
    
    -- Product-level aggregations  
    SUM(quantity) OVER (PARTITION BY product_id) AS product_total_quantity,
    SUM(total_amount) OVER (PARTITION BY product_id) AS product_total_sales,
    
    -- Daily aggregations
    SUM(total_amount) OVER (
      PARTITION BY DATE(sale_timestamp)
    ) AS daily_total_sales,
    
    -- Ranking
    ROW_NUMBER() OVER (
      PARTITION BY store_id 
      ORDER BY total_amount DESC
    ) AS store_sale_rank,
    
    -- Metadata
    CURRENT_TIMESTAMP AS processed_at,
    _loaded_at

FROM {{ ref('int_sales_enriched') }}