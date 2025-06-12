{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Merchant information staging
-- Prepares merchant data for side input join equivalent
SELECT 
    merchant_number,
    COALESCE(merchant_name, '{{ var("default_value") }}') AS merchant_name,
    COALESCE(merchant_category_code, '{{ var("default_value") }}') AS merchant_category_code,
    COALESCE(merchant_country_code, 'GB') AS merchant_country_code,
    COALESCE(merchant_city, '{{ var("default_value") }}') AS merchant_city,
    COALESCE(merchant_state, '{{ var("default_value") }}') AS merchant_state,
    COALESCE(merchant_zip_code, '{{ var("default_value") }}') AS merchant_zip_code,
    COALESCE(merchant_phone, '{{ var("default_value") }}') AS merchant_phone,
    COALESCE(merchant_dba_name, '{{ var("default_value") }}') AS merchant_dba_name,
    active_flag
    
FROM {{ source('transformed_layer', 'dim_merchant_information') }}

WHERE 
    active_flag = '{{ var("active_flag") }}'
    AND merchant_number IS NOT NULL