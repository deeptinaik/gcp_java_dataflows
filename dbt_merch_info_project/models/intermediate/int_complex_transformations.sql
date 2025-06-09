{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

-- Advanced model demonstrating complex BigQuery to Snowflake conversions
-- Including UNPIVOT operations, array aggregations, and complex joins

WITH dda_unpivot AS (
  -- Snowflake equivalent of BigQuery UNPIVOT operation
  SELECT 
    merchant_number,
    hash_value,
    sequence_number
  FROM {{ ref('stg_dimension_merch_info_temp') }}
  UNPIVOT (
    hash_value FOR sequence_number IN (
      dda1_last4_hash_key AS 1,
      dda2_last4_hash_key AS 2,
      dda3_last4_hash_key AS 3,
      dda4_last4_hash_key AS 4
    )
  )
),

owner_unpivot AS (
  -- Complex UNPIVOT for owner information
  SELECT 
    merchant_number,
    owner_name,
    primary_id_hash,
    primary_sec_id_rk,
    sequence_number
  FROM {{ ref('stg_dimension_merch_info_temp') }}
  UNPIVOT (
    (owner_name, primary_id_hash, primary_sec_id_rk) FOR sequence_number IN (
      (primary_owner_name, primary_id_hash_key, primary_id_rk) AS 1,
      (secondary_owner_name, secondary_primary_id_hash_key, secondary_primary_id_rk) AS 2
    )
  )
),

xl_layer_union AS (
  -- Union of all XL layer tables for DDA data
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ukrg_mmfndeu_supplement_funding_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'UKRG' AS source_system
  FROM {{ source('xl_layer', 'ukrg_mmfndeu_supplement_funding') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ap_mmddsq_dda_tbl_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'AP' AS source_system
  FROM {{ source('xl_layer', 'ap_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ca_mmddsq_dda_tbl_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'CA' AS source_system
  FROM {{ source('xl_layer', 'ca_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('us_mmddsq_dda_tbl_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'US' AS source_system
  FROM {{ source('xl_layer', 'us_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('indir_mmddsq_dda_tbl_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'IND' AS source_system
  FROM {{ source('xl_layer', 'indir_mmddsq_dda_tbl') }}
),

tl_layer_sens_union AS (
  -- Union of all TL layer sensitive tables for DDA data
  SELECT 
    {{ safe_trim('dda_nbr') }} AS dda_nbr,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ukrg_mmfndeu_supplement_funding') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('nbr') }} AS dda_nbr,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ap_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('nbr') }} AS dda_nbr,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ca_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('nbr') }} AS dda_nbr,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'us_mmddsq_dda_tbl') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('nbr') }} AS dda_nbr,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'indir_mmddsq_dda_tbl') }}
),

dda_enriched AS (
  -- Join unpivoted DDA data with XL and TL layers
  SELECT
    du.merchant_number,
    du.sequence_number,
    tls.dda_nbr,
    {{ mask_last_four('tls.dda_nbr') }} AS dda_number_rk,
    xl.source_system,
    xl.etlbatchid
  FROM dda_unpivot du
  LEFT JOIN xl_layer_union xl 
    ON du.merchant_number = xl.merchant_number 
    AND du.hash_value = xl.hash_key_value
  LEFT JOIN tl_layer_sens_union tls 
    ON xl.hash_key_value = tls.hash_key_value 
    AND tls.etlbatchid = CAST(xl.etlbatchid AS STRING)
  WHERE du.hash_value IS NOT NULL
),

dda_array_final AS (
  -- Create final DDA array structure
  SELECT
    merchant_number,
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'sequence_number', CAST(sequence_number AS STRING),
        'dda_nbr', dda_nbr,
        'dda_number_rk', dda_number_rk
      )
    ) WITHIN GROUP (ORDER BY sequence_number) AS dda_array
  FROM dda_enriched
  WHERE dda_nbr IS NOT NULL
  GROUP BY merchant_number
),

owner_layer_union AS (
  -- Union of all owner tables from XL layer
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ukrg_mmownq_own_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'UKRG' AS source_system
  FROM {{ source('xl_layer', 'ukrg_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ap_mmownq_own_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'AP' AS source_system
  FROM {{ source('xl_layer', 'ap_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('ca_mmownq_own_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'CA' AS source_system
  FROM {{ source('xl_layer', 'ca_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('us_mmownq_own_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'US' AS source_system
  FROM {{ source('xl_layer', 'us_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('merchant_number') }} AS merchant_number,
    {{ safe_trim('indir_mmownq_own_sk') }} AS table_sk,
    etlbatchid,
    hash_key_value,
    'IND' AS source_system
  FROM {{ source('xl_layer', 'indir_mmownq_own') }}
),

owner_sens_union AS (
  -- Union of all owner sensitive tables from TL layer
  SELECT 
    {{ safe_trim('primary_id') }} AS primary_id,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ukrg_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('primary_id') }} AS primary_id,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ap_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('primary_id') }} AS primary_id,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'ca_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('primary_id') }} AS primary_id,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'us_mmownq_own') }}
  
  UNION ALL
  
  SELECT 
    {{ safe_trim('primary_id') }} AS primary_id,
    etlbatchid,
    hash_key_value
  FROM {{ source('tl_layer_sens', 'indir_mmownq_own') }}
),

owner_enriched AS (
  -- Join unpivoted owner data with XL and TL layers
  SELECT
    ou.merchant_number,
    ou.sequence_number,
    ou.owner_name,
    ou.primary_sec_id_rk,
    ols.primary_id,
    ol.source_system,
    ol.etlbatchid
  FROM owner_unpivot ou
  LEFT JOIN owner_layer_union ol 
    ON ou.merchant_number = ol.merchant_number 
    AND ou.primary_id_hash = ol.hash_key_value
  LEFT JOIN owner_sens_union ols 
    ON ol.hash_key_value = ols.hash_key_value 
    AND ols.etlbatchid = CAST(ol.etlbatchid AS STRING)
  WHERE ou.primary_id_hash IS NOT NULL
),

owner_array_final AS (
  -- Create final owner information array structure
  SELECT
    merchant_number,
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'sequence_number', CAST(sequence_number AS STRING),
        'owner_name', owner_name,
        'primary_id', primary_id,
        'primary_sec_id_rk', primary_sec_id_rk
      )
    ) WITHIN GROUP (ORDER BY sequence_number) AS owner_information_array
  FROM owner_enriched
  WHERE primary_id IS NOT NULL
  GROUP BY merchant_number
)

-- Final select bringing everything together
SELECT
    temp.merchant_number,
    temp.etlbatchid,
    temp.merchant_information_sk,
    temp.hierarchy,
    temp.dba_name,
    temp.legal_name,
    temp.company_name,
    temp.retail_name,
    temp.additional_optional_data,
    COALESCE(daf.dda_array, ARRAY_CONSTRUCT()) AS dda_array_detailed,
    COALESCE(oaf.owner_information_array, ARRAY_CONSTRUCT()) AS owner_information_array_detailed,
    temp.geographical_region,
    temp.merchant_status,
    temp.purge_flag,
    temp.cdc_hash_key
FROM {{ ref('stg_dimension_merch_info_temp') }} temp
LEFT JOIN dda_array_final daf ON temp.merchant_number = daf.merchant_number
LEFT JOIN owner_array_final oaf ON temp.merchant_number = oaf.merchant_number