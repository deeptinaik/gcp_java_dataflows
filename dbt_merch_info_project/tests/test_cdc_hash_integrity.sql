-- Custom test to validate change data capture hash integrity
SELECT 
    merchant_number,
    change_data_capture_hash,
    cdc_hash_key
FROM {{ ref('int_youcm_gppn_temp') }}
WHERE 
    change_data_capture_hash IS NULL 
    OR LENGTH(change_data_capture_hash) = 0
HAVING COUNT(*) > 0