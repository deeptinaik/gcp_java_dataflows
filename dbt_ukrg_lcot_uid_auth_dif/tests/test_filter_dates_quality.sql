-- Test data quality for filter dates staging model

-- Test that etlbatchid is not null
SELECT 'etlbatchid_not_null' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
WHERE etlbatchid IS NULL

UNION ALL

-- Test that filter dates are reasonable (not in future, not too far in past)
SELECT 'filter_dates_reasonable' AS test_name, COUNT(*) AS failure_count  
FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
WHERE filter_date_180_etlbatchid_date > CURRENT_DATE()
   OR filter_date_1_yr_etlbatchid_date_target < DATEADD(YEAR, -2, CURRENT_DATE())

UNION ALL

-- Test that etlbatchid format is correct (17 digits)
SELECT 'etlbatchid_format' AS test_name, COUNT(*) AS failure_count
FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
WHERE LENGTH(CAST(etlbatchid AS STRING)) <> 17