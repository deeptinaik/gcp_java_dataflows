{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for UKRG transaction fact view with date filtering and deduplication
-- Replicates the 7-day lookback logic and QUALIFY from original BigQuery query
WITH ranked_data AS (
  SELECT 
      uk_trans_ft_sk,
      etlbatchid,
      -- Add row number for deduplication (replaces QUALIFY in BigQuery)
      ROW_NUMBER() OVER (
          PARTITION BY uk_trans_ft_sk 
          ORDER BY etlbatchid DESC
      ) as rn
  FROM {{ source('transformed_layer', 'vw_ukrg_trans_fact') }}
  WHERE TRY_TO_DATE(SUBSTR(CAST(etlbatchid AS STRING), 1, 8), 'YYYYMMDD') 
        BETWEEN DATEADD(day, -{{ var('date_lookback_days') }}, CURRENT_DATE()) 
        AND CURRENT_DATE()
)

SELECT 
    uk_trans_ft_sk,
    etlbatchid,
    rn
FROM ranked_data
WHERE rn = 1