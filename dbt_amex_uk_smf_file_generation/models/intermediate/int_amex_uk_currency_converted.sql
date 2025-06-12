{{
    config(
        materialized='ephemeral'
    )
}}

-- Currency conversion processing
-- Replicates the CurrencyConversion.java logic
SELECT 
    transaction_id,
    merchant_number,
    transaction_date,
    transaction_time,
    transaction_amount,
    currency_code,
    
    -- Apply currency conversion logic
    CASE 
        WHEN needs_currency_conversion = TRUE THEN
            {{ convert_currency('transaction_amount', 'currency_code', 'COALESCE(settlement_currency, "' + var("gbp_currency") + '")') }}
        ELSE COALESCE(settlement_amount, transaction_amount)
    END AS calculated_settlement_amount,
    
    COALESCE(settlement_currency, '{{ var("gbp_currency") }}') AS final_settlement_currency,
    
    card_number,
    card_type,
    authorization_code,
    batch_number,
    terminal_id,
    
    -- Amex specific fields (cleaned values from staging)
    amex_se_number_clean AS amex_se_number,
    amex_merchant_id_clean AS amex_merchant_id,
    amex_card_member_number,
    amex_transaction_code_clean AS amex_transaction_code,
    amex_reference_number_clean AS amex_reference_number,
    amex_approval_code_clean AS amex_approval_code,
    amex_charge_date_clean AS amex_charge_date,
    amex_submission_date_clean AS amex_submission_date,
    
    -- Enriched merchant information
    enriched_merchant_name,
    enriched_merchant_category_code,
    enriched_merchant_country_code,
    enriched_merchant_city,
    enriched_merchant_state,
    enriched_merchant_zip_code,
    enriched_merchant_phone,
    enriched_merchant_dba_name,
    
    -- Processing metadata
    etl_batch_date,
    create_date_time,
    update_date_time,
    
    -- Add conversion metadata (equivalent to Java conversion logic)
    {{ get_exchange_rate('currency_code', 'COALESCE(settlement_currency, "' + var("gbp_currency") + '")') }} AS conversion_rate,
    CURRENT_TIMESTAMP() AS conversion_timestamp,
    '{{ var("success_status") }}' AS conversion_status,
    
    -- Processing flags
    amex_processed_flag,
    processing_timestamp,
    merchant_enrichment_status,
    needs_currency_conversion,
    processing_split_timestamp
    
FROM {{ ref('int_amex_uk_processing_split') }}