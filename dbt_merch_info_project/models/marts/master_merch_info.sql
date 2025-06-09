{{
    config(
        materialized='incremental',
        unique_key='merchant_number',
        merge_update_columns=[
            'update_date_time', 'merchant_sequence_key', 'hierarchy', 'dba_name', 
            'legal_name', 'company_name', 'retail_name', 'additional_optional_data',
            'contact_array', 'email_array', 'phone_number_array', 'dda_array',
            'company_tax_id', 'company_tax_id_rk', 'owner_information_array',
            'geographical_region', 'cdc_hash_key', 'merchant_status',
            'purge_flag', 'dba_address_array'
        ],
        schema='marts'
    )
}}

-- Final mart model equivalent to master_merch_info table
-- Handles incremental updates using MERGE strategy

SELECT
    {{ generate_uuid() }} AS master_merch_info_sk,
    etlbatchid,
    {{ parse_date_from_string('YYYYMMDD', 'substr(cast(etlbatchid as string), 1, 8)') }} AS etl_batch_date,
    {{ current_datetime() }} AS create_date_time,
    {{ current_datetime() }} AS update_date_time,
    merchant_sequence_key,
    hierarchy,
    merchant_number,
    dba_name,
    legal_name,
    company_name,
    retail_name,
    additional_optional_data,
    contact_array,
    email_array,
    phone_number_array,
    dda_array,
    tax_identification_number AS company_tax_id,
    company_tax_id_rk,
    owner_information_array,
    '' AS owner_phone_number,
    'GPN' AS acquirer_name,
    geographical_region_final AS geographical_region,
    0 AS current_ind,
    change_data_capture_hash AS cdc_hash_key,
    merchant_status_code_mapped AS merchant_status,
    purge_flag,
    dba_address_array
FROM {{ ref('int_youcm_gppn_temp') }}

{% if is_incremental() %}
  -- Only process new or changed records
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} target
    WHERE target.merchant_number = int_youcm_gppn_temp.merchant_number
      AND COALESCE(target.cdc_hash_key, '') = COALESCE(int_youcm_gppn_temp.change_data_capture_hash, '')
  )
{% endif %}