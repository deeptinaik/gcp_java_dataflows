-- Test data quality for auth-dif matching

-- Test that primary matching fields are not null
SELECT 'primary_fields_not_null' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
WHERE auth_sk IS NULL 
   OR trans_sk IS NULL
   OR card_number_hk IS NULL
   OR merchant_number IS NULL

UNION ALL

-- Test that joinind values are within expected range
SELECT 'joinind_values_valid' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
WHERE joinind NOT BETWEEN 0.1 AND 100

UNION ALL

-- Test that transaction amounts are positive when not null
SELECT 'transaction_amounts_positive' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
WHERE transaction_amount IS NOT NULL 
  AND transaction_amount <= 0

UNION ALL

-- Test that lcot_guid_key_sk follows UUID format when not null
SELECT 'lcot_guid_format' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
WHERE lcot_guid_key_sk IS NOT NULL 
  AND LENGTH(lcot_guid_key_sk) <> 36

UNION ALL

-- Test that global_trid is populated when available
SELECT 'global_trid_populated' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
WHERE global_trid_source IS NOT NULL 
  AND (global_trid IS NULL OR global_trid = '')