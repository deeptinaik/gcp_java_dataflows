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
            
            -- Supplemental authorization amount match
            WHEN 
                merchant_number_t = merchant_number_a 
                AND (amount_t1 = amount_a1 AND arnp1 = trnp12)
                AND tran_date_t = tran_date_a
                AND tran_time_t = tran_time_a
                AND card_nbr_t = card_nbr_a
                AND auth_code_t = auth_code_a
                AND tran_id_t = tran_id_a
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '2_{{ var("default_joinind_date") }}'
            
            -- Authorization amount match
            WHEN 
                merchant_number_t = merchant_number_a 
                AND (arnp1 = trnp13 AND amount_t2 = amount_a) 
                AND tran_date_t = tran_date_a 
                AND tran_time_t = tran_time_a 
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND tran_id_t = tran_id_a 
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '3_{{ var("default_joinind_date") }}'
            
            -- Transaction ID trimmed match scenarios
            WHEN
                merchant_number_t = merchant_number_a 
                AND (amount_t = amount_a AND arnp1 = trnp11) 
                AND tran_date_t = tran_date_a 
                AND tran_time_t = tran_time_a 
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND transaction_id_trim_last_char = tran_id_a
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '4_{{ var("default_joinind_date") }}'
            
            -- Time window matching (4 character time)
            WHEN 
                merchant_number_t = merchant_number_a
                AND (amount_t = amount_a AND arnp11 = trnp41)
                AND tran_date_t = tran_date_a
                AND tran_time_1_4_t = tran_time_1_4_a
                AND card_nbr_t = card_nbr_a
                AND auth_code_t = auth_code_a
                AND tran_id_t = tran_id_a 
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '7_{{ var("default_joinind_date") }}'
            
            -- ACOMPOS specific matching
            WHEN
                merchant_number_t = merchant_number_a 
                AND (amount_t = amount_a AND arnp23 = trnp6)
                AND auth_date_t = tran_date_a
                AND ref_number_t = retrieval_ref_number_a
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND tran_id_t = tran_id_a 
                AND card_type_t = '37' 
                AND record_type_gnap_auth = 'ACOMPOS'
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '11.1_{{ var("default_joinind_date") }}'
            
            -- Partial matching scenarios
            WHEN 
                merchant_number_t = merchant_number_a 
                AND ((amount_t = amount_a AND arnp2 = trnp21) OR (amount_t1 = amount_a1 AND arnp2 = trnp22) OR (amount_t2 = amount_a AND arnp2 = trnp23)) 
                AND tran_date_t = tran_date_a 
                AND tran_time_1_4_t = tran_time_1_4_a
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND tran_id_1_4_t = tran_id_1_4_a 
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '19_{{ var("default_joinind_date") }}'
            
            -- Empty transaction ID scenarios
            WHEN 
                merchant_number_t = merchant_number_a
                AND ((amount_t = amount_a AND arnp7 = trnp61) OR (amount_t1 = amount_a1 AND arnp7 = trnp62) OR (arnp7 = trnp63 AND amount_t2 = amount_a)) 
                AND tran_date_t = tran_date_a 
                AND tran_time_t = tran_time_a 
                AND card_nbr_t = card_nbr_a 
                AND auth_code_t = auth_code_a 
                AND (tran_id_t = '' OR tran_id_a = '') 
                AND {{ terminal_id_match('terminal_id_t', 'terminal_id_a') }}
            THEN '22_{{ var("default_joinind_date") }}'
            
            -- Default case for non-matches
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
            AND trans_sk NOT IN (SELECT DISTINCT trans_sk FROM auth_dif_join_ind_all_1)
    ) T
    ON merchant_number_t = merchant_number_a
    AND card_nbr_t = card_nbr_a
    AND (
        (tran_id_t = '' OR tran_id_a = '')
        OR (tran_id_t = tran_id_last_6_1_4_a)
        OR (tran_id_1_4_t = tran_id_1_4_a)
        OR (tran_id_t = tran_id_last_4_a)
        OR (lenght_tran_id_t >= 4)
    )
),

auth_dif_join_ind AS (
    SELECT * FROM auth_dif_join_ind_all_1 
    UNION ALL 
    SELECT * FROM (
        SELECT 
            * EXCEPT (joinind_joininddate),
            CAST({{ split_safe_offset('joinind_joininddate', '_', '0') }} AS NUMBER) AS joinind,
            {{ split_safe_offset('joinind_joininddate', '_', '1') }} AS joininddate  
        FROM auth_dif_join_ind_all_2
    )
    WHERE joinind <> 100 OR CONCAT(trans_sk, north_uk_auth_fin_sk) IS NULL
),

validrow AS (
    SELECT
        * EXCEPT (trans_sk, trans_sk_original),
        trans_sk_original AS trans_sk,
        MIN(joinind) OVER (PARTITION BY north_uk_auth_fin_sk) AS jid,
        MIN(joinind) OVER (PARTITION BY trans_sk) AS jid2
    FROM (
        SELECT
            * EXCEPT(trans_sk),
            trans_sk AS trans_sk_original,
            COALESCE(trans_sk, {{ generate_uuid() }}) AS trans_sk
        FROM auth_dif_join_ind T 
    ) validrow_01
)

SELECT
    DISTINCT 
    north_uk_auth_fin_sk AS auth_sk,
    trans_sk,
    COALESCE(card_number_rk_t, card_number_rk_a) AS card_number_rk,
    card_nbr_a AS card_number_hk,
    COALESCE(card_number_sk_original_a, card_number_sk_original_t) AS card_number_sk_original,
    COALESCE(tran_date_t, tran_date_a) AS transaction_date,
    COALESCE(merchant_number_t, merchant_number_a) AS merchant_number,
    COALESCE(amount_t, amount_a) AS transaction_amount,
    COALESCE(corp_t, corp_a) AS corp,
    COALESCE(region_t, region_a) AS region,
    COALESCE(principal_t, principal_a) AS principal,
    COALESCE(associate_t, associate_a) AS associate,
    COALESCE(chain_t, chain_a) AS chain,
    tran_id_a_original AS banknet_trace_id_tran,
    merchant_dba_name_a AS merchant_dba_name,
    COALESCE(lcot_guid_key_sk_tran, lcot_guid_key_sk_auth) AS lcot_guid_key_sk,
    COALESCE(global_trid_diff, global_trid_auth) AS global_trid,
    COALESCE(global_trid_source_diff, global_trid_source_auth) AS global_trid_source,
    etlbatchid_tran,
    etlbatchid_auth,
    joinind,
    joininddate,
    global_trid_source_diff,
    global_trid_source_auth,
    global_trid_diff,
    global_trid_auth,
    global_trid_target,
    ROW_NUMBER() OVER(PARTITION BY trans_sk ORDER BY north_uk_auth_fin_sk DESC) AS max_row

FROM validrow
WHERE 
    (joinind = jid2)
    AND (
        (trans_sk IS NOT NULL AND trans_sk <> '-1')
        OR etlbatchid_auth > (SELECT filter_date_150_days_etlbatchid_auth FROM {{ ref('stg_uk_auth_dif_filter_dates') }})
    )

{% if is_incremental() %}
    AND {{ current_date() }} > (SELECT MAX(joininddate) FROM {{ this }})
{% endif %}