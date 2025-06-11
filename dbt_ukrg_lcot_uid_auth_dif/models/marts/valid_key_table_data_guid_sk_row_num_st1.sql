{{
    config(
        materialized='incremental',
        schema='backup_xl_layer',
        unique_key=['north_uk_auth_fin_sk', 'trans_sk'],
        merge_update_columns=['joinind_auth_dif', 'joininddate_auth_dif', 'global_trid_source_dif', 'global_trid_source_tds', 'global_trid_dif', 'global_trid_tds']
    )
}}

-- Auth-Dif matching logic with complex business rules
-- Converted from lcot_query_4 in ukrg_lcot_uid_auth_dif.sh

WITH auth_dif_join_ind_all_1 AS (
    SELECT 
        *,
        CAST(0.1 AS NUMBER) AS joinind,
        '{{ var("default_joinind_date") }}' AS joininddate
    FROM (
        SELECT * 
        FROM {{ ref('stg_temp_uk_auth_table') }}
        WHERE 
            TRIM(COALESCE(global_trid_auth, '')) <> '' 
            AND {{ auth_response_code_valid('resp_code_num_gnap_auth', 'record_type_gnap_auth', 'rec_type_gnap_auth', 'excep_reason_code_gnap_auth') }}
    ) A
    
    JOIN (
        SELECT * 
        FROM {{ ref('stg_temp_uk_dif_table') }} 
        WHERE 
            card_scheme_diff <> 'OB'
            AND TRIM(COALESCE(global_trid_diff, '')) <> ''
    ) T
    ON A.global_trid_auth = T.global_trid_diff 
    AND A.global_trid_source_auth = T.global_trid_source_diff
),

auth_dif_join_ind_all_2 AS (
    SELECT
        *,
        CASE
            -- Exact match scenario 1: Full field matching
            WHEN 
                merchant_number_t = merchant_number_a 
                AND (amount_t = amount_a AND arnp1 = trnp11) 
                AND tran_date_t = tran_date_a 
                AND tran_time_t = tran_time_a 
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND tran_id_t = tran_id_a
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '1_{{ var("default_joinind_date") }}'
            
            -- Default fallback
            ELSE '100_{{ var("default_joinind_date") }}'
        END AS joinind_joininddate

    FROM (
        SELECT * 
        FROM {{ ref('stg_temp_uk_auth_table') }}
        WHERE {{ auth_response_code_valid('resp_code_num_gnap_auth', 'record_type_gnap_auth', 'rec_type_gnap_auth', 'excep_reason_code_gnap_auth') }}
    ) A
    JOIN (
        SELECT * 
        FROM {{ ref('stg_temp_uk_dif_table') }} 
        WHERE 
            card_scheme_diff <> 'OB'
    ) T
    ON merchant_number_t = merchant_number_a
    AND card_nbr_t = card_nbr_a
),

auth_dif_join_ind AS (
    SELECT * FROM auth_dif_join_ind_all_1 
    UNION ALL 
    SELECT * FROM auth_dif_join_ind_all_2
    WHERE trans_sk NOT IN (SELECT DISTINCT trans_sk FROM auth_dif_join_ind_all_1)
)

SELECT 
    -- Key fields for uniqueness
    COALESCE(north_uk_auth_fin_sk, '{{ var("default_sk_value") }}') AS north_uk_auth_fin_sk,
    COALESCE(trans_sk, '{{ var("default_sk_value") }}') AS trans_sk,
    
    -- Business logic fields
    CAST({{ split_safe_offset('joinind_joininddate', '_', 0) }} AS NUMERIC) AS joinind_auth_dif,
    {{ split_safe_offset('joinind_joininddate', '_', 1) }} AS joininddate_auth_dif,
    
    -- Global TRID fields
    global_trid_diff AS global_trid_source_dif,  
    global_trid_auth AS global_trid_source_tds,
    global_trid_diff AS global_trid_dif,
    global_trid_auth AS global_trid_tds,
    
    -- Additional fields
    {{ var('etl_batch_id') }} AS etlbatchid_auth,
    {{ var('etl_batch_id') }} AS etlbatchid_tran,
    
    -- LCOT GUID generation
    {{ select_lcot_guid_key_sk('trans_sk', 'north_uk_auth_fin_sk', 'lcot_guid_key_sk_trans', 'lcot_guid_key_sk_auth') }} AS lcot_guid_key_sk

FROM auth_dif_join_ind
