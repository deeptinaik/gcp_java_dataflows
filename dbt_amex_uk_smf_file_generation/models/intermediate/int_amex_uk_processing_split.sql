{{
    config(
        materialized='ephemeral'
    )
}}

-- SMF Data Processing split equivalent
-- Replicates the SMFDataProcessing.java logic for splitting currency conversion vs direct processing
SELECT 
    *,
    
    -- Determine if currency conversion is needed (equivalent to needsCurrencyConversion method)
    CASE 
        WHEN currency_code != COALESCE(settlement_currency, '{{ var("gbp_currency") }}') THEN TRUE
        WHEN COALESCE(settlement_currency, '{{ var("gbp_currency") }}') != '{{ var("gbp_currency") }}' THEN TRUE
        WHEN settlement_amount IS NULL THEN TRUE
        WHEN TRY_CAST(settlement_amount AS DECIMAL(15,2)) = 0 THEN TRUE
        ELSE FALSE
    END AS needs_currency_conversion,
    
    -- Add processing metadata
    CURRENT_TIMESTAMP() AS processing_split_timestamp
    
FROM {{ ref('int_amex_uk_enriched_transactions') }}