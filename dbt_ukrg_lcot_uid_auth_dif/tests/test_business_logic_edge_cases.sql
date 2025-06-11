-- Test for complex business logic validation and edge cases

-- Test that staging models have consistent row counts
SELECT 'staging_models_consistency' AS test_name, COUNT(*) AS failure_count
FROM (
    SELECT 'auth_table' AS model_name, COUNT(*) AS row_count 
    FROM {{ ref('stg_temp_uk_auth_table') }}
    UNION ALL
    SELECT 'dif_table' AS model_name, COUNT(*) AS row_count 
    FROM {{ ref('stg_temp_uk_dif_table') }}
) staging_counts
WHERE row_count = 0

UNION ALL

-- Test that filter dates are logically consistent
SELECT 'filter_dates_logical_order' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
WHERE filter_date_180_etlbatchid_date > filter_date_5_days_etlbatchid_date_auth
   OR filter_date_5_days_etlbatchid_date_auth > filter_date_150_etlbatchid_date_auth

UNION ALL

-- Test that terminal IDs follow expected patterns when not null
SELECT 'terminal_id_patterns' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_temp_uk_auth_table') }}
WHERE terminal_id_a IS NOT NULL 
  AND LENGTH(TRIM(terminal_id_a)) > 0
  AND NOT REGEXP_LIKE(TRIM(terminal_id_a), '^[A-Z0-9]{1,8}$')

UNION ALL

-- Test that card numbers are properly masked/handled
SELECT 'card_number_security' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_temp_uk_auth_table') }}
WHERE card_nbr_a IS NOT NULL 
  AND LENGTH(card_nbr_a) > 0
  AND REGEXP_LIKE(card_nbr_a, '[0-9]{16}') -- Should not contain full PANs

UNION ALL

-- Test that authorization response codes are valid
SELECT 'auth_response_codes_valid' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_temp_uk_auth_table') }}
WHERE resp_code_num_gnap_auth IS NOT NULL
  AND (resp_code_num_gnap_auth < 0 OR resp_code_num_gnap_auth > 999)

UNION ALL

-- Test for duplicate records in final marts model
SELECT 'marts_no_duplicates' AS test_name, COUNT(*) AS failure_count
FROM (
    SELECT north_uk_auth_fin_sk, trans_sk, COUNT(*) as record_count
    FROM {{ ref('valid_key_table_data_guid_sk_row_num_st1') }}
    GROUP BY north_uk_auth_fin_sk, trans_sk
    HAVING COUNT(*) > 1
) duplicates