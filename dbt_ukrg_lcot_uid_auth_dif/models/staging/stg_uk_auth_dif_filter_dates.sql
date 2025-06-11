{{
    config(
        materialized='view',
        schema='backup_xl_layer'
    )
}}

-- Filter dates calculation for UKRG LCOT UID Auth Dif processing
-- Converted from lcot_query_1 in ukrg_lcot_uid_auth_dif.sh

WITH lcot_max_etlbatchids AS (
    SELECT 
        COALESCE(
            {{ parse_date('%Y%m%d', 'SUBSTR(CAST(MAX(etlbatchid_dif) AS STRING), 1, 8)') }}, 
            {{ current_date() }}
        ) AS max_dif_date,
        COALESCE(
            {{ parse_date('%Y%m%d', 'SUBSTR(CAST(MAX(etlbatchid_auth) AS STRING), 1, 8)') }}, 
            {{ current_date() }}
        ) AS max_auth_date
    FROM xl_layer.lcot_uid_key_ukrg
)

SELECT 
    {{ var('etl_batch_id') }} AS etlbatchid,
    
    CAST({{ parse_date('%Y%m%d', 'SUBSTR(CAST(' ~ var('etl_batch_id') | string ~ ' AS STRING), 1, 8)') }} AS DATE) AS etlbatchid_date,
    
    -- Filter dates for dif processing (10 days back)
    CAST({{ rpad(format_date('%Y%m%d', date_sub('max_dif_date', var('filter_date_interval_dif'), 'DAY')), '17', '0') }} AS NUMBER) AS filter_date_etlbatchid_dif,
    
    -- Filter dates for auth processing (150 days back)
    CAST({{ rpad(format_date('%Y%m%d', date_sub('max_auth_date', var('filter_date_interval_auth_150'), 'DAY')), '17', '0') }} AS NUMBER) AS filter_date_150_days_etlbatchid_auth,
    {{ date_sub('max_auth_date', var('filter_date_interval_auth_150'), 'DAY') }} AS filter_date_150_etlbatchid_date_auth,
    
    -- Filter dates for auth processing (5 days back)
    {{ date_sub('max_auth_date', var('filter_date_interval_auth_5'), 'DAY') }} AS filter_date_5_days_etlbatchid_date_auth,
    CAST({{ rpad(format_date('%Y%m%d', date_sub('max_auth_date', var('filter_date_interval_auth_5'), 'DAY')), '17', '0') }} AS NUMBER) AS filter_date_5_days_etlbatchid_auth,
    
    -- Additional filter dates
    {{ date_sub('max_dif_date', var('filter_date_interval_180'), 'DAY') }} AS filter_date_180_etlbatchid_date,
    CAST({{ rpad(format_date('%Y%m%d', date_sub('max_dif_date', var('filter_date_interval_auth_5'), 'DAY')), '17', '0') }} AS NUMBER) AS etlbatchid_tran_full_join,
    {{ date_sub('max_dif_date', var('filter_date_interval_365'), 'DAY') }} AS filter_date_1_yr_etlbatchid_date_target,
    {{ date_sub(current_date(), var('filter_date_interval_20'), 'DAY') }} AS filter_date_20_etlbatchid_date_guid,
    {{ date_sub(current_date(), var('filter_date_interval_auth_150'), 'DAY') }} AS filter_date_150_etlbatchid_date_guid

FROM lcot_max_etlbatchids