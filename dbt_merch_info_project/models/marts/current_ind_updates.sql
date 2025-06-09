{{
    config(
        materialized='incremental',
        unique_key=['merchant_number', 'cdc_hash_key'],
        schema='marts',
        post_hook="UPDATE {{ ref('master_merch_info') }} SET current_ind = 1 WHERE (merchant_number, cdc_hash_key) IN (SELECT merchant_number, cdc_hash_key FROM {{ this }} WHERE update_insert_flag = 1)"
    )
}}

-- Model to handle current_ind updates using incremental strategy
-- Equivalent to update_current_ind MERGE operation in the original script

SELECT
    change_data_capture_hash,
    cdc_hash_key,
    merchant_number,
    update_insert_flag
FROM {{ ref('int_youcm_gppn_temp') }}
WHERE update_insert_flag = 1

{% if is_incremental() %}
  AND (merchant_number, cdc_hash_key) NOT IN (
    SELECT merchant_number, cdc_hash_key FROM {{ this }}
  )
{% endif %}