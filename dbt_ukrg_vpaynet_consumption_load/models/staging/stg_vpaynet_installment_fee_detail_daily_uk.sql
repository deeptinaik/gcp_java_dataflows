{{
  config(
    materialized='view',
    schema='staging_layer'
  )
}}

-- Staging model for VPayNet installment fee detail daily UK
-- Simple pass-through with data type consistency for Snowflake
SELECT 
    vpaynet_installment_fee_detail_sk,
    arn,
    merchant_number,
    vpaynet_installment_plan_id,
    installment_plan_name,
    installment_transaction_status,
    plan_frequency,
    number_of_installments,
    plan_promotion_code,
    plan_acceptance_created_date_time,
    transaction_amount,
    transaction_currency_code,
    clearing_amount,
    clearing_currency_code,
    clearing_date_time,
    cancelled_amount,
    cancelled_currency_code,
    derived_flag,
    cancelled_date_time,
    installment_funding_fee_amount,
    installment_funding_fee_currency_code,
    installments_service_fee_eligibility_amount,
    installments_service_fee_eligibility_currency_code,
    funding_type,
    etlbatchid
FROM {{ source('transformed_layer', 'vpaynet_installment_fee_detail_daily_uk') }}