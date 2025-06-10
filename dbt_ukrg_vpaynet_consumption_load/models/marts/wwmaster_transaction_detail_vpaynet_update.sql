{{
  config(
    materialized='incremental',
    unique_key='transaction_detail_sk',
    merge_update_columns=['vp_vpaynet_installment_fee_detail_sk', 'vp_vpaynet_installment_ind', 'vp_arn', 'vp_merchant_number', 'vp_vpaynet_installment_plan_id', 'vp_installment_plan_name', 'vp_installment_transaction_status', 'vp_plan_frequency', 'vp_number_of_installments', 'vp_plan_promotion_code', 'vp_plan_acceptance_created_date_time', 'vp_transaction_amount', 'vp_transaction_currency_code', 'vp_clearing_amount', 'vp_clearing_currency_code', 'vp_clearing_date_time', 'vp_cancelled_amount', 'vp_cancelled_currency_code', 'vp_derived_flag', 'vp_cancelled_date_time', 'vp_installment_funding_fee_amount', 'vp_installment_funding_fee_currency_code', 'vp_installments_service_fee_eligibility_amount', 'vp_installments_service_fee_eligibility_currency_code', 'vp_funding_type', 'vp_etlbatchid'],
    schema='consumption_layer'
  )
}}

-- Main transformation model for VPayNet consumption load
-- Converts BigQuery MERGE operation to DBT incremental model
-- Replicates the complex join logic and field mappings from the original script

WITH source_data AS (
  SELECT 
    A.etlbatchid,
    B.trans_sk,
    A.uk_trans_ft_sk,
    
    -- Handle null vpaynet installment fee detail sk with macro
    {{ handle_null_vpaynet_sk('src.vpaynet_installment_fee_detail_sk') }} AS vpaynet_installment_fee_detail_sk,
    
    -- Generate vpaynet installment indicator with macro
    {{ get_vpaynet_installment_indicator('B.vpaynet_installment_fee_detail_sk_vp') }} AS vp_vpaynet_installment_ind,
    
    -- VPayNet fields with vp_ prefix to match target table
    src.arn AS vp_arn,
    src.merchant_number AS vp_merchant_number,
    src.vpaynet_installment_plan_id AS vp_vpaynet_installment_plan_id,
    src.installment_plan_name AS vp_installment_plan_name,
    src.installment_transaction_status AS vp_installment_transaction_status,
    src.plan_frequency AS vp_plan_frequency,
    src.number_of_installments AS vp_number_of_installments,
    src.plan_promotion_code AS vp_plan_promotion_code,
    src.plan_acceptance_created_date_time AS vp_plan_acceptance_created_date_time,
    src.transaction_amount AS vp_transaction_amount,
    src.transaction_currency_code AS vp_transaction_currency_code,
    src.clearing_amount AS vp_clearing_amount,
    src.clearing_currency_code AS vp_clearing_currency_code,
    src.clearing_date_time AS vp_clearing_date_time,
    src.cancelled_amount AS vp_cancelled_amount,
    src.cancelled_currency_code AS vp_cancelled_currency_code,
    src.derived_flag AS vp_derived_flag,
    src.cancelled_date_time AS vp_cancelled_date_time,
    src.installment_funding_fee_amount AS vp_installment_funding_fee_amount,
    src.installment_funding_fee_currency_code AS vp_installment_funding_fee_currency_code,
    src.installments_service_fee_eligibility_amount AS vp_installments_service_fee_eligibility_amount,
    src.installments_service_fee_eligibility_currency_code AS vp_installments_service_fee_eligibility_currency_code,
    src.funding_type AS vp_funding_type,
    src.etlbatchid AS vp_etlbatchid
    
  FROM {{ ref('stg_lotr_uid_key_ukrg') }} B
  INNER JOIN {{ ref('stg_ukrg_trans_fact') }} A 
    ON A.uk_trans_ft_sk = B.trans_sk
  LEFT JOIN {{ ref('stg_vpaynet_installment_fee_detail_daily_uk') }} src 
    ON src.vpaynet_installment_fee_detail_sk = B.vpaynet_installment_fee_detail_sk_vp
  
  -- Only include records from staging models that passed deduplication
  WHERE B.rn = 1 AND A.rn = 1
)

SELECT 
  uk_trans_ft_sk AS transaction_detail_sk,
  vpaynet_installment_fee_detail_sk AS vp_vpaynet_installment_fee_detail_sk,
  vp_vpaynet_installment_ind,
  vp_arn,
  vp_merchant_number,
  vp_vpaynet_installment_plan_id,
  vp_installment_plan_name,
  vp_installment_transaction_status,
  vp_plan_frequency,
  vp_number_of_installments,
  vp_plan_promotion_code,
  vp_plan_acceptance_created_date_time,
  vp_transaction_amount,
  vp_transaction_currency_code,
  vp_clearing_amount,
  vp_clearing_currency_code,
  vp_clearing_date_time,
  vp_cancelled_amount,
  vp_cancelled_currency_code,
  vp_derived_flag,
  vp_cancelled_date_time,
  vp_installment_funding_fee_amount,
  vp_installment_funding_fee_currency_code,
  vp_installments_service_fee_eligibility_amount,
  vp_installments_service_fee_eligibility_currency_code,
  vp_funding_type,
  vp_etlbatchid
FROM source_data

{% if is_incremental() %}
  -- When running incrementally, only process records that might have changed
  -- This replaces the MERGE logic from the original BigQuery script
  WHERE transaction_detail_sk IN (
    SELECT uk_trans_ft_sk 
    FROM {{ ref('stg_ukrg_trans_fact') }}
    WHERE rn = 1
  )
{% endif %}