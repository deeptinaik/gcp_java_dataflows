-- Test for data validation - equivalent to AmountValidator, LocationValidator, etc.
-- Should return no records if all data is valid

SELECT
    transaction_id,
    amount,
    location,
    customer_id,
    {{ validate_transaction_data('amount', 'location', 'customer_id') }} AS validation_status
FROM {{ ref('stg_transactions') }}
WHERE {{ validate_transaction_data('amount', 'location', 'customer_id') }} != 'VALID'