-- Test that all Amex card numbers start with 3
-- This replicates the validation logic from AmexTransactionFilter.java

SELECT *
FROM {{ ref('stg_amex_uk_filtered_transactions') }}
WHERE LEFT(card_number, 1) != '3'