{{
    config(
        materialized='table',
        schema='marts',
        unique_key='transaction_id',
        post_hook="INSERT INTO {{ this.schema }}.amex_uk_smf_processing_log (batch_date, records_processed, processing_timestamp) VALUES ('{{ var("etl_batch_date") }}', (SELECT COUNT(*) FROM {{ this }}), CURRENT_TIMESTAMP())"
    )
}}

-- SMF File Generation Final Output
-- Replicates the FileFormatting.java logic for final SMF format
SELECT 
    -- SMF Header information (equivalent to FileFormatting.java)
    '{{ var("smf_transaction_record") }}' AS smf_record_type,
    LPAD(ROW_NUMBER() OVER (ORDER BY transaction_date, transaction_time, transaction_id), 10, '0') AS smf_sequence_number,
    '{{ var("etl_batch_date") }}' AS smf_file_date,
    '{{ var("etl_batch_date") }}' AS smf_processing_date,
    '{{ var("active_flag") }}' AS smf_settlement_flag,
    
    -- Transaction identification (formatted according to SMF specs)
    {{ format_string('transaction_id', var('transaction_id_length')) }} AS transaction_id,
    {{ format_string('merchant_number', var('max_merchant_number_length')) }} AS merchant_number,
    {{ format_string('amex_se_number', 10) }} AS amex_se_number,
    {{ format_string('amex_merchant_id', 15) }} AS amex_merchant_id,
    {{ format_string('amex_reference_number', 20) }} AS amex_reference_number,
    
    -- Transaction details (formatted)
    TO_VARCHAR(transaction_date, 'YYYY-MM-DD') AS transaction_date,
    COALESCE(TO_VARCHAR(transaction_time, 'HH24:MI:SS'), '00:00:00') AS transaction_time,
    {{ format_amount('transaction_amount') }} AS transaction_amount,
    {{ format_amount('calculated_settlement_amount') }} AS settlement_amount,
    {{ format_string('currency_code', 3) }} AS currency_code,
    {{ format_string('final_settlement_currency', 3) }} AS settlement_currency,
    
    -- Card information (masked for security)
    {{ mask_card_number('card_number') }} AS card_number,
    {{ format_string('card_type', 10) }} AS card_type,
    {{ format_string('authorization_code', 10) }} AS authorization_code,
    
    -- Merchant information (formatted)
    {{ format_string('enriched_merchant_name', 40) }} AS merchant_name,
    {{ format_string('enriched_merchant_category_code', 4) }} AS merchant_category_code,
    {{ format_string('enriched_merchant_country_code', 3) }} AS merchant_country_code,
    {{ format_string('enriched_merchant_city', 25) }} AS merchant_city,
    {{ format_string('enriched_merchant_state', 10) }} AS merchant_state,
    
    -- Amex specific fields (formatted)
    {{ format_string('amex_transaction_code', 10) }} AS amex_transaction_code,
    {{ format_string('amex_approval_code', 10) }} AS amex_approval_code,
    TO_VARCHAR(amex_charge_date, 'YYYY-MM-DD') AS amex_charge_date,
    TO_VARCHAR(amex_submission_date, 'YYYY-MM-DD') AS amex_submission_date,
    
    -- Processing metadata
    etl_batch_date,
    TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') AS create_date_time,
    '1.0' AS smf_format_version,
    '{{ var("success_status") }}' AS processing_status,
    
    -- Additional fields for audit and tracking
    conversion_rate,
    merchant_enrichment_status,
    needs_currency_conversion,
    CURRENT_TIMESTAMP() AS smf_generation_timestamp,
    
    -- Create SMF file identifier
    CONCAT('{{ var("file_prefix_amex_uk") }}', 
           TO_VARCHAR(CURRENT_DATE(), 'YYYYMMDD'), 
           '_', 
           LPAD(ROW_NUMBER() OVER (ORDER BY transaction_date, transaction_time, transaction_id), 6, '0'),
           '{{ var("file_extension_smf") }}') AS smf_file_identifier
    
FROM {{ ref('int_amex_uk_currency_converted') }}

-- Apply final filters and ordering
ORDER BY 
    transaction_date, 
    transaction_time, 
    transaction_id