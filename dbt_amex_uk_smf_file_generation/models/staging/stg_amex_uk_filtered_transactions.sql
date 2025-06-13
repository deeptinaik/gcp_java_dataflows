{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Amex transaction filter equivalent
-- Replicates the AmexTransactionFilter.java logic
SELECT 
    *,
    -- Enrich with Amex-specific processing flags
    '{{ var("active_flag") }}' AS amex_processed_flag,
    CURRENT_TIMESTAMP() AS processing_timestamp,
    
    -- Set default values for missing Amex fields (equivalent to setDefaultAmexValues method)
    COALESCE(amex_se_number, '0000000') AS amex_se_number_clean,
    COALESCE(amex_merchant_id, merchant_number) AS amex_merchant_id_clean,
    COALESCE(amex_transaction_code, 'SALE') AS amex_transaction_code_clean,
    COALESCE(amex_reference_number, transaction_id) AS amex_reference_number_clean,
    COALESCE(amex_approval_code, authorization_code) AS amex_approval_code_clean,
    COALESCE(amex_charge_date, transaction_date) AS amex_charge_date_clean,
    COALESCE(amex_submission_date, CURRENT_DATE()) AS amex_submission_date_clean
    
FROM {{ ref('stg_amex_uk_validated_transactions') }}

WHERE 
    -- Check if card type is Amex (equivalent to isAmexTransaction method)
    UPPER(card_type) IN ('AMEX', 'AMERICAN EXPRESS')
    
    -- Check if card number follows Amex pattern (starts with 3)
    AND card_number IS NOT NULL
    AND LEFT(card_number, 1) = '3'
    AND LENGTH(card_number) = {{ var('amex_card_number_length') }}
    
    -- Check if merchant accepts Amex
    AND merchant_number IS NOT NULL
    AND TRIM(merchant_number) != ''