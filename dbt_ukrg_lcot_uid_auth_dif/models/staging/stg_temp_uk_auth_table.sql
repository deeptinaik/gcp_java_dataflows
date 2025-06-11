{{
    config(
        materialized='view',
        schema='backup_xl_layer'
    )
}}

-- Authorization data preparation for Auth-Dif matching
-- Converted from lcot_query_3 in ukrg_lcot_uid_auth_dif.sh

WITH filter_dates AS (
    SELECT 
        filter_date_5_days_etlbatchid_auth,
        filter_date_150_days_etlbatchid_auth,
        filter_date_150_etlbatchid_date_auth,
        filter_date_5_days_etlbatchid_date_auth
    FROM {{ ref('stg_uk_auth_dif_filter_dates') }}
),

auth_base AS (
    SELECT DISTINCT
        north_uk_authorization_fin_ft_sk AS north_uk_auth_fin_sk,
        {{ ltrim_zeros('AA.merchant_number') }} AS merchant_number,
        AA.amount_1x,
        REPLACE(COALESCE(TRIM(UPPER(network_reference_number)), ''), ' ', '') AS network_reference_number,
        network_reference_number AS transaction_id_original,
        COALESCE(TRIM(UPPER(AA.approval_code)), '') AS approval_code,
        AA.tran_date,
        AA.card_number_sk AS card_number_sk_original_a,
        AA.card_number_sk AS card_number_hk,
        AA.card_number_rk,
        REPLACE(TRIM(CAST(AA.tran_time AS STRING)), ':', '') AS tran_time,
        AA.terminal_id,
        AA.sequence_number,
        CAST(response_code AS NUMBER) AS resp_code_num_gnap_auth,
        settle_date_month,
        record_type AS record_type_gnap_auth,
        rec_type AS rec_type_gnap_auth,
        excep_reason_code AS excep_reason_code_gnap_auth,
        retrieval_ref_number AS retrieval_ref_number_auth,
        AA.lcot_guid_key_sk AS lcot_guid_key_sk_auth,
        H.corporate AS corp,
        H.region,
        H.principal,
        H.associate,
        H.chain,
        H.dba_name AS merchant_name,
        AA.etlbatchid AS etlbatchid_auth,
        TRIM(global_trid) AS global_trid_auth,
        TRIM(global_trid_source) AS global_trid_source_auth,
        global_trid_target

    FROM (
        SELECT 
            AA.north_uk_authorization_fin_ft_sk,
            {{ ltrim_zeros('AA.merchant_number') }} AS merchant_number,
            AA.amount_1x,
            AA.network_reference_number,
            AA.approval_code,
            AA.tran_date,
            AA.global_token,
            AA.card_number_sk,
            AA.card_number_rk,
            AA.tran_time,
            AA.terminal_id,
            AA.sequence_number,
            AA.response_code,
            AA.settle_date_month,
            AA.record_type,
            AA.rec_type,
            AA.excep_reason_code,
            AA.retrieval_ref_number,
            AA.etlbatchid,
            AA.global_trid,
            AA.global_trid_source,
            AUTH.lcot_guid_key_sk,
            global_trid_target
        FROM (
            SELECT 
                north_uk_authorization_fin_ft_sk,
                merchant_number,
                amount_1x,
                network_reference_number,
                approval_code,
                tran_date,
                card_number_sk,
                global_token,
                card_number_rk,
                tran_time,
                terminal_id,
                sequence_number,
                response_code,
                settle_date_month,
                record_type,
                rec_type,
                excep_reason_code,
                retrieval_ref_number,
                etlbatchid,
                global_trid AS global_trid_target,
                {{ generate_global_trid_auth('amount_1x', 'network_reference_number', 'merchant_number', 'tran_date', 'global_trid') }} AS global_trid,
                COALESCE(global_trid_source, '') AS global_trid_source
            FROM {{ source('xl_layer', 'vw_north_uk_authorization_fin_ft') }} 
            CROSS JOIN filter_dates
            WHERE etl_batch_date >= filter_dates.filter_date_150_etlbatchid_date_auth
            QUALIFY ROW_NUMBER() OVER(PARTITION BY north_uk_authorization_fin_ft_sk ORDER BY etlbatchid DESC) = 1
        ) AA
        
        JOIN (
            SELECT 
                GUID_BASE.auth_sk,
                MAX(GUID_BASE.lcot_guid_key_sk) AS lcot_guid_key_sk
            FROM (
                SELECT 
                    GUID_AUTH_DIF.auth_sk, 
                    lcot_guid_key_sk 
                FROM (
                    SELECT 
                        guid_auth.auth_sk, 
                        guid_auth.lcot_guid_key_sk
                    FROM (
                        SELECT 
                            auth_sk,
                            merchant_number,
                            card_number_sk,
                            MAX(CASE WHEN trans_sk = '-1' THEN lcot_guid_key_sk ELSE NULL END) AS lcot_guid_key_sk
                        FROM {{ source('xl_layer', 'lcot_uid_key_ukrg') }} 
                        CROSS JOIN filter_dates
                        WHERE 
                            auth_sk <> '-1'
                            AND etlbatchid_auth >= filter_dates.filter_date_150_days_etlbatchid_auth
                        GROUP BY auth_sk, merchant_number, card_number_sk
                    ) guid_auth
                    
                    JOIN (	
                        SELECT 
                            merchant_number_t, 
                            card_nbr_t 
                        FROM {{ ref('stg_temp_uk_dif_table') }}
                        GROUP BY merchant_number_t, card_nbr_t
                    ) dif
                    ON guid_auth.merchant_number = dif.merchant_number_t
                    AND guid_auth.card_number_sk = dif.card_nbr_t
                ) GUID_AUTH_DIF
                
                UNION ALL 
                
                SELECT 
                    BASE.auth_sk, 
                    BASE.lcot_guid_key_sk 
                FROM (
                    SELECT 
                        north_uk_authorization_fin_ft_sk AS auth_sk,
                        NULL AS lcot_guid_key_sk  
                    FROM {{ source('xl_layer', 'vw_north_uk_authorization_fin_ft') }} 
                    CROSS JOIN filter_dates
                    WHERE etl_batch_date >= filter_dates.filter_date_5_days_etlbatchid_date_auth
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY north_uk_authorization_fin_ft_sk ORDER BY etlbatchid DESC) = 1
                ) BASE
                
                LEFT JOIN (
                    SELECT auth_sk 
                    FROM {{ source('xl_layer', 'lcot_uid_key_ukrg') }} 
                    CROSS JOIN filter_dates
                    WHERE 
                        auth_sk <> '-1' 
                        AND etlbatchid_auth >= filter_dates.filter_date_5_days_etlbatchid_auth
                    GROUP BY auth_sk
                ) GUID ON BASE.auth_sk = GUID.auth_sk
                WHERE GUID.auth_sk IS NULL	
            ) GUID_BASE
            GROUP BY auth_sk
        ) AUTH ON AA.north_uk_authorization_fin_ft_sk = AUTH.auth_sk

    ) AA
    
    LEFT OUTER JOIN (
        SELECT
            corporate, 
            region, 
            principal, 
            associate, 
            chain, 
            dba_name, 
            {{ ltrim_zeros('merchant_number') }} AS merchant_number
        FROM {{ source('consumption_layer', 'dim_merchant_information') }}
        WHERE current_ind = '0'
    ) H ON AA.merchant_number = H.merchant_number
),

auth_with_analytics AS (
    SELECT
        north_uk_auth_fin_sk,
        merchant_number AS merchant_number_a,
        (amount_1x) / 100 AS amount_a,
        amount_1x AS amount_a1,
        network_reference_number AS tran_id_a,
        SUBSTR(network_reference_number, 1, 4) AS tran_id_1_4_a,
        SUBSTR(SUBSTR(network_reference_number, LENGTH(network_reference_number) - 5), 1, 4) AS tran_id_last_6_1_4_a,
        SUBSTR(network_reference_number, LENGTH(network_reference_number) - 3) AS tran_id_last_4_a,
        SUBSTR(network_reference_number, 1, 1) AS tran_id_first_letter_a,
        transaction_id_original AS tran_id_a_original,
        approval_code AS auth_code_a,
        SUBSTR(SUBSTR(approval_code, LENGTH(approval_code) - 3), 1, 4) AS auth_code_last_4_a,
        SUBSTR(SUBSTR(approval_code, LENGTH(approval_code) - 1), 1, 2) AS auth_code_last_2_a,
        tran_date AS tran_date_a,
        card_number_sk_original_a,
        card_number_hk AS card_nbr_a,
        card_number_rk AS card_number_rk_a,
        tran_time AS tran_time_a,
        SUBSTR(tran_time, 1, 4) AS tran_time_1_4_a,
        resp_code_num_gnap_auth,
        settle_date_month AS settle_date_month_a,
        record_type_gnap_auth,
        rec_type_gnap_auth,
        excep_reason_code_gnap_auth,
        retrieval_ref_number_auth AS retrieval_ref_number_a,
        {{ date_add('tran_date', '8', 'DAY') }} AS tran_date_after_8_days_a,
        {{ date_sub('tran_date', '10', 'DAY') }} AS tran_date_before_10_days_a,
        {{ date_add('tran_date', '10', 'DAY') }} AS tran_date_after_10_days_a,
        {{ date_add('tran_date', '30', 'DAY') }} AS tran_date_after_30_days_a,
        corp AS corp_a,
        region AS region_a,
        principal AS principal_a,
        associate AS associate_a,
        chain AS chain_a,
        merchant_name AS merchant_dba_name_a,
        COALESCE(terminal_id, '') AS terminal_id_a,
        lcot_guid_key_sk_auth,
        etlbatchid_auth,
        etlbatchid_auth AS etlbatchid_auth_main,
        global_trid_auth,
        global_trid_source_auth,
        global_trid_target,
        
        -- Authorization ranking for various matching scenarios
        {{ auth_ranking_partition('merchant_number', 'amount_1x', 'network_reference_number', 'approval_code', 'tran_date', 'SUBSTR(tran_time, 1, 6)', 'card_number_hk', 'terminal_id, sequence_number, north_uk_auth_fin_sk DESC') }} AS arnp1,
        {{ auth_ranking_partition('merchant_number', 'amount_1x', 'network_reference_number', 'approval_code', 'tran_date', 'card_number_hk', 'tran_time, terminal_id, sequence_number, north_uk_auth_fin_sk DESC') }} AS arnp2,
        {{ auth_ranking_partition('merchant_number', 'amount_1x', 'network_reference_number', 'tran_date', 'card_number_hk', 'tran_time, terminal_id, sequence_number, north_uk_auth_fin_sk DESC') }} AS arnp3
        
        -- Additional ranking patterns would continue here for all ARNP variations from original query
        
    FROM auth_base
)

SELECT * FROM auth_with_analytics