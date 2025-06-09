{{
  config(
    materialized='view',
    description='Staging table for retail sales data'
  )
}}

-- Raw sales data from file ingestion
-- Equivalent to input files in RetailDataflowJava pipeline

SELECT
    sale_id,
    product_id,
    store_id,
    quantity,
    total_amount,
    sale_timestamp,
    _loaded_at,
    _source_file
FROM {{ source('raw_data', 'sales_data') }}
WHERE _loaded_at IS NOT NULL