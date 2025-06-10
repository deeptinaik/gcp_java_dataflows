-- Test to validate that domestic/international classification is working correctly
SELECT *
FROM {{ ref('mybank_itmz_dtl_uk') }}
WHERE domestic_international_ind NOT IN ('DOMESTIC', 'INTERNATIONAL')