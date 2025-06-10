-- Test to validate currency amount calculations are not null or zero when expected
SELECT *
FROM {{ ref('mybank_itmz_dtl_uk') }}
WHERE amount IS NULL 
   OR expected_settled_amount IS NULL
   OR (amount = 0 AND expected_settled_amount = 0)