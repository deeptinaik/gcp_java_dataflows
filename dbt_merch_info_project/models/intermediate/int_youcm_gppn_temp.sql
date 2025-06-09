{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

-- Intermediate transformation model equivalent to gpn_temp_table_load in BigQuery script
-- Contains complex business logic with change data capture hash calculation

WITH base_merchant_data AS (
  SELECT
    etlbatchid,
    merchant_information_sk AS merchant_sequence_key,
    hierarchy,
    merchant_number,
    dba_name,
    legal_name,
    company_name,
    retail_name,
    additional_optional_data,
    merchant_contact_name,
    primary_email_address,
    secondary_email_address,
    officer_phone_number,
    dba_phone_number,
    phone_number_array,
    dda_array,
    owner_information_array,
    email_array,
    contact_array,
    geographical_region,
    tax_identification_number,
    cdc_hash_key,
    merchant_status_code,
    purge_flag,
    dba_address_array,
    dba_address1,
    dba_address2,
    dba_city,
    dba_state,
    dba_country_code,
    dba_zip,
    {{ safe_trim('merchant_number') }} AS clean_merchant_number,
    {{ safe_trim('hierarchy') }} AS clean_hierarchy,
    {{ safe_trim('dba_name') }} AS clean_dba_name,
    {{ safe_trim('legal_name') }} AS clean_legal_name,
    {{ safe_trim('company_name') }} AS clean_company_name,
    {{ safe_trim('retail_name') }} AS clean_retail_name,
    {{ safe_trim('additional_optional_data') }} AS clean_additional_optional_data,
    {{ safe_trim('merchant_contact_name') }} AS clean_merchant_contact_name,
    {{ safe_trim('primary_email_address') }} AS clean_primary_email_address,
    {{ safe_trim('secondary_email_address') }} AS clean_secondary_email_address,
    {{ safe_trim('officer_phone_number') }} AS clean_officer_phone_number,
    {{ safe_trim('dba_phone_number') }} AS clean_dba_phone_number,
    {{ safe_trim('merchant_information_sk') }} AS clean_merchant_information_sk,
    {{ safe_trim('merchant_status') }} AS clean_merchant_status,
    {{ safe_trim('purge_flag') }} AS clean_purge_flag,
    {{ safe_trim('dba_address1') }} AS clean_dba_address1,
    {{ safe_trim('dba_address2') }} AS clean_dba_address2,
    {{ safe_trim('dba_city') }} AS clean_dba_city,
    {{ safe_trim('dba_state') }} AS clean_dba_state,
    {{ safe_trim('dba_country_ind') }} AS clean_dba_country_code,
    {{ safe_trim('dba_postal_code') }} AS clean_dba_zip,
    
    -- Tax identification number with masking
    {{ mask_last_four('tax_identification_number') }} AS company_tax_id_rk,
    
    -- Merchant status code transformation
    CASE
      WHEN UPPER({{ safe_trim('merchant_status') }}) IN ('D', 'C', '', NULL) THEN 'CLOSED'
      WHEN UPPER({{ safe_trim('merchant_status') }}) IN ('F') THEN 'INACTIVE'
      WHEN UPPER({{ safe_trim('merchant_status') }}) IN ('O', 'R') THEN 'OPEN'
      WHEN UPPER({{ safe_trim('merchant_status') }}) IN ('P') THEN 'PENDING'
      ELSE 'UNKNOWN'
    END AS merchant_status_code_mapped,
    
    -- Geographical region with default handling
    CASE
      WHEN geographical_region IS NULL THEN 'UN'
      WHEN geographical_region IS NOT NULL THEN geographical_region
    END AS geographical_region_final
    
  FROM {{ ref('stg_dimension_merch_info_temp') }} mt
  LEFT JOIN (
    SELECT 
      DISTINCT corporate,
      region,
      geographical_region
    FROM {{ source('cnsmp_layer', 'merchant_geo_location') }}
  ) a ON a.corporate = mt.corporate AND a.region = mt.region
),

-- Contact array aggregation (simplified for Snowflake)
contact_arrays AS (
  SELECT
    merchant_number,
    ARRAY_AGG(OBJECT_CONSTRUCT(
      'sequence_number', sequence_number,
      'contact_name', contact_name
    )) AS contact_array
  FROM (
    SELECT 
      DISTINCT {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('merchant_contact_name') }} AS contact_name,
      1 AS sequence_number
    FROM {{ ref('stg_dimension_merch_info_temp') }}
    WHERE merchant_contact_name IS NOT NULL
  )
  GROUP BY merchant_number
),

-- Email array aggregation (simplified for Snowflake)
email_arrays AS (
  SELECT
    merchant_number,
    ARRAY_AGG(OBJECT_CONSTRUCT(
      'sequence_number', sequence_number,
      'email_address', email_address
    )) AS email_array
  FROM (
    SELECT 
      {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('primary_email_address') }} AS email_address,
      1 AS sequence_number
    FROM {{ ref('stg_dimension_merch_info_temp') }}
    WHERE primary_email_address IS NOT NULL
    
    UNION ALL
    
    SELECT 
      {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('secondary_email_address') }} AS email_address,
      2 AS sequence_number
    FROM {{ ref('stg_dimension_merch_info_temp') }}
    WHERE secondary_email_address IS NOT NULL
  )
  GROUP BY merchant_number
),

-- Phone array aggregation (simplified for Snowflake)
phone_arrays AS (
  SELECT
    merchant_number,
    ARRAY_AGG(OBJECT_CONSTRUCT(
      'phone_number_ind', phone_number_ind,
      'phone_number', phone_number
    )) AS phone_number_array
  FROM (
    SELECT 
      {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('officer_phone_number') }} AS phone_number,
      'DBA' AS phone_number_ind
    FROM {{ ref('stg_dimension_merch_info_temp') }}
    WHERE officer_phone_number IS NOT NULL
    
    UNION ALL
    
    SELECT 
      {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('dba_phone_number') }} AS phone_number,
      'ADDITIONAL' AS phone_number_ind
    FROM {{ ref('stg_dimension_merch_info_temp') }}
    WHERE dba_phone_number IS NOT NULL
  )
  GROUP BY merchant_number
),

-- DBA address array aggregation (simplified for Snowflake)
dba_address_arrays AS (
  SELECT
    merchant_number,
    ARRAY_AGG(OBJECT_CONSTRUCT(
      'dba_address1', dba_address1,
      'dba_address2', dba_address2,
      'dba_city', dba_city,
      'dba_state', dba_state,
      'dba_country_ind', dba_country_ind,
      'dba_postal_code', dba_postal_code
    )) AS dba_address_array
  FROM (
    SELECT 
      DISTINCT {{ safe_trim('merchant_number') }} AS merchant_number,
      {{ safe_trim('dba_address1') }} AS dba_address1,
      {{ safe_trim('dba_address2') }} AS dba_address2,
      {{ safe_trim('dba_city') }} AS dba_city,
      {{ safe_trim('dba_state') }} AS dba_state,
      {{ safe_trim('dba_country_ind') }} AS dba_country_ind,
      {{ safe_trim('dba_postal_code') }} AS dba_postal_code
    FROM {{ ref('stg_dimension_merch_info_temp') }}
  )
  GROUP BY merchant_number
),

-- CDC hash key from existing records
existing_cdc_hash AS (
  SELECT
    DISTINCT {{ safe_trim('merchant_number') }} AS merchant_number,
    cdc_hash_key
  FROM {{ source('cnsmp_layer_sensitive', 'master_merch_info') }}
  WHERE current_ind = 0 AND acquirer_name = 'GPN'
),

-- Final transformation with hash calculation
final_transformation AS (
  SELECT
    bmd.*,
    ca.contact_array,
    ea.email_array,
    pa.phone_number_array,
    daa.dba_address_array,
    ech.cdc_hash_key,
    
    -- Change data capture hash calculation (simplified for demo)
    {{ hash_md5_base64("
      COALESCE(bmd.clean_hierarchy, '') ||
      COALESCE(bmd.clean_merchant_number, '') ||
      COALESCE(bmd.clean_dba_name, '') ||
      COALESCE(bmd.clean_legal_name, '') ||
      COALESCE(bmd.clean_merchant_contact_name, '') ||
      COALESCE(bmd.clean_primary_email_address, '') ||
      COALESCE(bmd.clean_secondary_email_address, '') ||
      COALESCE(bmd.clean_officer_phone_number, '') ||
      COALESCE(bmd.clean_dba_phone_number, '') ||
      COALESCE(CAST(bmd.tax_identification_number AS STRING), '') ||
      COALESCE(bmd.merchant_status_code_mapped, '') ||
      COALESCE(bmd.clean_dba_address1, '') ||
      COALESCE(bmd.clean_dba_address2, '') ||
      COALESCE(bmd.clean_dba_city, '') ||
      COALESCE(bmd.clean_dba_state, '') ||
      COALESCE(bmd.clean_dba_country_code, '') ||
      COALESCE(bmd.clean_dba_zip, '')
    ") }} AS change_data_capture_hash
    
  FROM base_merchant_data bmd
  LEFT JOIN contact_arrays ca ON {{ safe_trim('bmd.merchant_number') }} = ca.merchant_number
  LEFT JOIN email_arrays ea ON {{ safe_trim('bmd.merchant_number') }} = ea.merchant_number
  LEFT JOIN phone_arrays pa ON {{ safe_trim('bmd.merchant_number') }} = pa.merchant_number
  LEFT JOIN dba_address_arrays daa ON {{ safe_trim('bmd.merchant_number') }} = daa.merchant_number
  LEFT JOIN existing_cdc_hash ech ON {{ safe_trim('bmd.merchant_number') }} = ech.merchant_number
)

-- Final select with update_insert_flag calculation
SELECT
    *,
    CASE
      WHEN COALESCE(cdc_hash_key, '') = COALESCE(change_data_capture_hash, '') THEN 0
      ELSE 1
    END AS update_insert_flag
FROM final_transformation