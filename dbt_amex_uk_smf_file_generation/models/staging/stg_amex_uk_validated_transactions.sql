{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Data validation transform equivalent
-- Replicates the DataValidation.java logic
SELECT 
    transaction_id,
    merchant_number,
    transaction_date,
    transaction_time,
    transaction_amount,
    currency_code,
    settlement_amount,
    settlement_currency,
    card_number,
    card_type,
    authorization_code,
    batch_number,
    terminal_id,
    amex_se_number,
    amex_merchant_id,
    amex_card_member_number,
    amex_transaction_code,
    amex_reference_number,
    amex_approval_code,
    amex_charge_date,
    amex_submission_date,
    etl_batch_date,
    create_date_time,
    update_date_time,
    
    -- Add validation metadata (equivalent to Java validation logic)
    CURRENT_TIMESTAMP() AS validation_timestamp,
    '{{ var("success_status") }}' AS validation_status
    
FROM {{ source('trusted_layer', 'amex_uk_transactions') }}

WHERE 
    -- Validate required fields (equivalent to isValidTransaction method)
    transaction_id IS NOT NULL 
    AND TRIM(transaction_id) != ''
    AND merchant_number IS NOT NULL 
    AND TRIM(merchant_number) != ''
    AND transaction_date IS NOT NULL
    AND transaction_amount IS NOT NULL
    
    -- Validate transaction amount is positive
    AND TRY_CAST(transaction_amount AS DECIMAL(15,2)) > 0
    
    -- Validate merchant number length
    AND LENGTH(merchant_number) >= {{ var('min_merchant_number_length') }}
    AND LENGTH(merchant_number) <= {{ var('max_merchant_number_length') }}
    
    -- Validate transaction ID length
    AND LENGTH(transaction_id) = {{ var('transaction_id_length') }}
    
    -- Filter by ETL batch date
    AND etl_batch_date = '{{ var("etl_batch_date") }}'