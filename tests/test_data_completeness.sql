-- Test that no data is lost during processing
-- Compare record counts between staging and mart tables

WITH staging_counts AS (
  SELECT 'purchase_events' AS table_name, COUNT(*) AS record_count
  FROM {{ ref('stg_purchase_events') }}
  WHERE status = 'SUCCESS'
  
  UNION ALL
  
  SELECT 'transactions' AS table_name, COUNT(*) AS record_count
  FROM {{ ref('stg_transactions') }}
  
  UNION ALL
  
  SELECT 'sales_data' AS table_name, COUNT(*) AS record_count
  FROM {{ ref('stg_sales_data') }}
),

mart_counts AS (
  SELECT 'purchase_events' AS table_name, COUNT(*) AS record_count
  FROM {{ ref('int_purchase_events_enriched') }}
  
  UNION ALL
  
  SELECT 'transactions' AS table_name, 
         COUNT(*) AS record_count
  FROM (
    SELECT * FROM {{ ref('mart_suspicious_transactions') }}
    UNION ALL
    SELECT * FROM {{ ref('mart_normal_transactions') }}
  )
  
  UNION ALL
  
  SELECT 'sales_data' AS table_name, COUNT(*) AS record_count
  FROM {{ ref('mart_retail_sales') }}
)

SELECT 
  s.table_name,
  s.record_count AS staging_count,
  m.record_count AS mart_count,
  s.record_count - m.record_count AS difference
FROM staging_counts s
JOIN mart_counts m ON s.table_name = m.table_name
WHERE s.record_count != m.record_count