{{
    config(
        materialized='view',
        schema='backup_xl_layer'
    )
}}

-- Transaction data preparation for Auth-Dif matching
-- Converted from lcot_query_2 in ukrg_lcot_uid_auth_dif.sh

WITH filter_dates AS (
    SELECT filter_date_etlbatchid_dif
    FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
),

trans_base AS (
    SELECT
        T.ukrg_trans_fact_sk AS trans_sk,
        {{ ltrim_zeros('T.merchant_number') }} AS merchant_number,
        T.transaction_amount AS transaction_amount,
        T.supplemental_authorization_amount AS sup_authorization_amount,
        T.authorization_amount AS authorization_amount,
        REPLACE(COALESCE(TRIM(UPPER(T.transaction_identifier)), ''), ' ', '') AS transaction_id,
        {{ transaction_id_trim_last_char('T.transaction_identifier') }} AS transaction_id_trim_last_char,
        T.transaction_identifier AS transaction_id_original,
        COALESCE(TRIM(UPPER(T.authorization_code)), '') AS authorization_code,
        T.transaction_date,
        T.authorization_date,
        CAST(T.card_number_sk AS STRING) AS card_number_sk_original_t,
        CAST(T.card_number_sk AS STRING) AS card_number_hk,
        T.card_number_rk,
        REPLACE(TRIM(CAST(T.transaction_time AS STRING)), ':', '') AS transaction_time,
        TRIM(T.ref_number) AS ref_number,
        T.corporate AS corp,
        T.region,
        T.principal,
        T.associate,
        T.chain,
        T.merchant_name AS merchant_dba_name,
        T.original_transaction_ref_number AS original_transaction_reference,
        T.card_type,
        T.charge_type,
        T.transaction_code,
        T.all_source_terminal_id,
        T2.lcot_guid_key_sk AS lcot_guid_key_sk_tran,
        T.etlbatchid AS etlbatchid_tran,
        T.card_scheme AS card_scheme_diff,
        TRIM(T.global_trid) AS global_trid_diff,
        TRIM(T.global_trid_source) AS global_trid_source_diff

    FROM (
        SELECT
            ft.ukrg_trans_fact_sk,
            ft.merchant_number,
            ft.transaction_amount,
            ft.supplemental_authorization_amount,
            ft.authorization_amount,
            ft.transaction_identifier,
            ft.authorization_code,
            ft.transaction_date,
            ft.authorization_date,
            ft.card_number_sk,
            ft.card_number_rk,
            ft.transaction_time,
            ft.ref_number,
            ft.corporate,
            ft.region,
            ft.principal,
            ft.associate,
            ft.chain,
            ft.merchant_name,
            ft.original_transaction_ref_number,
            ft.card_type,
            ft.charge_type,
            ft.transaction_code,
            ft.all_source_terminal_id,
            ft.etlbatchid,
            ft.card_scheme,
            ft.global_trid,
            ft.global_trid_source
        FROM {{ source('xl_layer', 'vw_ukrg_tran_fact') }} ft 
        JOIN {{ ref('stg_uk_mpg_scorp') }} uk 
            ON uk.corporate != ft.corporate
        CROSS JOIN filter_dates
        WHERE
            ft.etlbatchid > filter_dates.filter_date_etlbatchid_dif  
            AND (ft.corporate NOT IN ('051','014') 
                 AND (ft.corporate NOT IN ('052') OR ft.region <> '05'))
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ukrg_trans_fact_sk ORDER BY etlbatchid DESC) = 1
    ) T
    
    LEFT JOIN (
        SELECT
            T1.trans_sk,
            T1.lcot_guid_key_sk,
            MAX(T1.auth_sk) AS auth_sk
        FROM {{ source('xl_layer', 'lcot_uid_key_ukrg') }} T1
        CROSS JOIN filter_dates
        WHERE
            T1.trans_sk <> '-1' 
            AND etlbatchid_dif >= filter_dates.filter_date_etlbatchid_dif
        GROUP BY T1.trans_sk, lcot_guid_key_sk
    ) T2 ON T.ukrg_trans_fact_sk = T2.trans_sk
    WHERE T2.trans_sk IS NULL OR T2.auth_sk = '-1'
),

trans_with_analytics AS (
    SELECT
        trans_sk,
        merchant_number AS merchant_number_t,
        transaction_amount AS amount_t,
        sup_authorization_amount AS amount_t1,
        authorization_amount AS amount_t2,
        transaction_id AS tran_id_t,
        card_type AS card_type_t,
        SUBSTR(transaction_id, 1, 4) AS tran_id_1_4_t,
        transaction_id_trim_last_char,
        SUBSTR(transaction_id, LENGTH(transaction_id) - 3) AS tran_id_last_4_t,
        transaction_id_original AS tran_id_t_original,
        LENGTH(transaction_id) AS lenght_tran_id_t,
        authorization_code AS auth_code_t,
        SUBSTR(SUBSTR(authorization_code, LENGTH(authorization_code) - 3), 1, 4) AS auth_code_last_4_t,
        SUBSTR(authorization_code, 1, 1) AS auth_code_first_letter_t,
        transaction_date AS tran_date_t,
        authorization_date AS auth_date_t,
        card_number_sk_original_t,
        card_number_hk AS card_nbr_t,
        card_number_rk AS card_number_rk_t,
        transaction_time AS tran_time_t,
        SUBSTR(transaction_time, 1, 4) AS tran_time_1_4_t,
        {{ date_sub(current_date(), '2', 'DAY') }} AS current_date_2_days,
        {{ date_sub(current_date(), '3', 'DAY') }} AS current_date_3_days,
        {{ date_sub(current_date(), '4', 'DAY') }} AS current_date_4_days,
        {{ date_sub(current_date(), '5', 'DAY') }} AS current_date_5_days,
        {{ date_add('transaction_date', '5', 'DAY') }} AS tran_date_after_5_days_t,
        original_transaction_reference,
        SUBSTR(original_transaction_reference, 1, 2) AS original_transaction_reference_1_2_t,
        SUBSTR(original_transaction_reference, 1, 4) AS original_transaction_reference_1_4_t,
        ref_number AS ref_number_t,
        corp AS corp_t,
        region AS region_t,
        principal AS principal_t,
        associate AS associate_t,
        chain AS chain_t,
        merchant_dba_name AS merchant_dba_name_t,
        charge_type AS charge_type_t,
        COALESCE(all_source_terminal_id, '') AS terminal_id_t,
        transaction_code AS transaction_code_t,
        lcot_guid_key_sk_tran,
        etlbatchid_tran,
        card_scheme_diff,
        global_trid_diff,
        global_trid_source_diff,
        
        -- Window functions for matching priority
        SUM(transaction_amount) OVER (PARTITION BY merchant_number, transaction_date, SUBSTR(transaction_time, 1, 4), card_number_hk, TRIM(authorization_code), TRIM(transaction_id)) AS sum_amount_t,
        SUM(transaction_amount) OVER (PARTITION BY merchant_number, SUBSTR(transaction_time, 1, 4), card_number_hk, TRIM(authorization_code), TRIM(transaction_id)) AS sum_amount_t1,
        SUM(transaction_amount) OVER (PARTITION BY merchant_number, card_number_hk, TRIM(authorization_code), TRIM(transaction_id)) AS sum_amount_t2,
        
        -- Transaction ranking for various matching scenarios
        {{ transaction_ranking_partition('merchant_number', 'transaction_amount', 'transaction_id', 'authorization_code', 'transaction_date', 'transaction_time', 'card_number_hk', 'original_transaction_reference, trans_sk DESC') }} AS trnp11,
        {{ transaction_ranking_partition('merchant_number', 'transaction_amount', 'transaction_id', 'authorization_code', 'transaction_date', 'card_number_hk', 'transaction_time, original_transaction_reference, trans_sk DESC') }} AS trnp21,
        {{ transaction_ranking_partition('merchant_number', 'transaction_amount', 'transaction_id', 'transaction_date', 'card_number_hk', 'transaction_time, original_transaction_reference, trans_sk DESC') }} AS trnp31
        
        -- Additional ranking patterns would continue here for all TRNP variations from original query
        
    FROM trans_base
)

SELECT * FROM trans_with_analytics