-- Test that merchant number length is within valid range
-- This replicates the validation logic from DataValidation.java

SELECT *
FROM {{ ref('stg_amex_uk_validated_transactions') }}
WHERE LENGTH(merchant_number) < {{ var('min_merchant_number_length') }}
   OR LENGTH(merchant_number) > {{ var('max_merchant_number_length') }}