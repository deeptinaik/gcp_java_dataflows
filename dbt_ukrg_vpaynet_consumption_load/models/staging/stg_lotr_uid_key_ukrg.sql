{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for LOTR UID Key UKRG table with deduplication logic
-- Replicates the QUALIFY logic from original BigQuery query
WITH ranked_data AS (
  SELECT 
      trans_sk,
      vpaynet_installment_fee_detail_sk_vp,
      etlbatchid,
      -- Add row number for deduplication (replaces QUALIFY in BigQuery)
      ROW_NUMBER() OVER (
          PARTITION BY trans_sk 
          ORDER BY vpaynet_installment_fee_detail_sk_vp DESC, etlbatchid DESC
      ) as rn
  FROM {{ source('transformed_layer', 'lotr_uid_key_ukrg') }}
  WHERE trans_sk != {{ var('default_sk_value') }}
)

SELECT 
    trans_sk,
    vpaynet_installment_fee_detail_sk_vp,
    etlbatchid,
    rn
FROM ranked_data
WHERE rn = 1