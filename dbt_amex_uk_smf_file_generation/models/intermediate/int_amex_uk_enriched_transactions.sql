{{
    config(
        materialized='ephemeral'
    )
}}

-- Merchant enrichment equivalent 
-- Replicates the MerchantEnrichment.java logic
SELECT 
    t.*,
    
    -- Enrich with merchant information (equivalent to side input join)
    COALESCE(m.merchant_name, '{{ var("default_value") }}') AS enriched_merchant_name,
    COALESCE(m.merchant_category_code, '{{ var("default_value") }}') AS enriched_merchant_category_code,
    COALESCE(m.merchant_country_code, 'GB') AS enriched_merchant_country_code,
    COALESCE(m.merchant_city, '{{ var("default_value") }}') AS enriched_merchant_city,
    COALESCE(m.merchant_state, '{{ var("default_value") }}') AS enriched_merchant_state,
    COALESCE(m.merchant_zip_code, '{{ var("default_value") }}') AS enriched_merchant_zip_code,
    COALESCE(m.merchant_phone, '{{ var("default_value") }}') AS enriched_merchant_phone,
    COALESCE(m.merchant_dba_name, '{{ var("default_value") }}') AS enriched_merchant_dba_name,
    
    -- Set enrichment status
    CASE 
        WHEN m.merchant_number IS NOT NULL THEN '{{ var("success_status") }}'
        ELSE '{{ var("failed_status") }}'
    END AS merchant_enrichment_status
    
FROM {{ ref('stg_amex_uk_filtered_transactions') }} t

LEFT JOIN {{ ref('stg_dim_merchant_information') }} m
    ON t.merchant_number = m.merchant_number