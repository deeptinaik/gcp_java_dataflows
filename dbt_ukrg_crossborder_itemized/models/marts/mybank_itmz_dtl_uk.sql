{{
    config(
        materialized='incremental',
        unique_key=['trans_sk_guid', 'type', 'sub_type', 'fee_type', 'status', 'exception_action_report_sk'],
        merge_update_columns=['etlbatchid', 'update_date_time', 'trans_sk_guid', 'chargeback_sk_chargeback_eod', 'member_balancing_sk', 'mybank_vp_mp_reclass_items_sk', 'exception_action_report_sk', 'type', 'sub_type', 'fee_type', 'geographical_region', 'mybank_acquirer_id', 'hierarchy', 'merchant_number', 'merchant_number_int', 'acquirer_bin', 'order_id', 'alpha_currency_code', 'currency_exponent', 'amount', 'country_code', 'reference_transaction_id', 'status', 'reason_text', 'payment_method', 'processing_date', 'file_date', 'merchant_aggregate_reference', 'bin_aggregate_reference', 'arn', 'settlement_alpha_currency_code', 'expected_settled_amount', 'settlement_currency_exponent', 'expected_settlement_date', 'scheme_rate', 'card_type', 'card_type_desc', 'charge_type', 'charge_type_desc', 'domestic_international_ind', 'interchange_fee_program', 'interchange_fee_program_desc', 'interchange_rate', 'vp_mp_rejects_sk', 'system_trace_audit_number', 'retrieval_ref_number', 'card_country_code', 'account_funding_source', 'card_bin', 'bin_detail_sk'],
        schema='transformed_layer_commplat',
        alias='mybank_itmz_dtl_uk'
    )
}}

-- Main mart model for UK crossborder itemized detail processing
-- This model replicates the MERGE operation from the original BigQuery script

WITH source_data AS (
    SELECT
        -- Primary keys and identifiers
        {{ generate_uuid() }} AS mybank_itemized_detail_sk,
        {{ generate_etl_batch_id() }} AS etlbatchid,
        {{ generate_current_timestamp() }} AS create_date_time,
        {{ generate_current_timestamp() }} AS update_date_time,
        trans_sk_guid,
        
        -- Static foreign keys
        COALESCE(chargeback_sk_chargeback_eod, '{{ var("default_sk_value") }}') AS chargeback_sk_chargeback_eod,
        '{{ var("default_sk_value") }}' AS member_balancing_sk,
        '{{ var("default_sk_value") }}' AS vp_mp_rejects_sk,
        '{{ var("default_sk_value") }}' AS mybank_vp_mp_reclass_items_sk,
        '{{ var("default_sk_value") }}' AS exception_action_report_sk,
        
        -- Static type classifications
        '{{ var("fee_type") }}' AS type,
        '{{ var("sub_type") }}' AS sub_type,
        '{{ var("fee_type_crossborder") }}' AS fee_type,
        
        -- Geographical region logic
        CASE
            WHEN dim_ctry.mybank_region IS NOT NULL THEN dim_ctry.mybank_geographical_region
            ELSE 'UK'
        END AS geographical_region,
        
        '{{ var("mybank_acquirer_id") }}' AS mybank_acquirer_id,
        transaction_id_diff AS transaction_id,
        
        -- Hierarchy construction
        {{ build_hierarchy('corp_diff', 'region_diff', 'principal_diff', 'associate_diff', 'chain_diff') }} AS hierarchy,
        
        merchant_number_diff AS merchant_number,
        
        -- Acquirer BIN logic
        COALESCE(
            CASE
                WHEN payment_method = 'mpaynet' THEN assc_parm1.mp_ica
                WHEN payment_method = 'vpaynet' THEN assc_parm1.vpaynet_bin
            END,
            SUBSTR(arn, 2, 6)
        ) AS acquirer_bin,
        
        {{ safe_cast('merchant_number_diff', 'INTEGER') }} AS merchant_number_int,
        
        -- Order ID construction
        {{ build_order_id('transaction_date_diff', 'sys_audit_nbr_diff', 'system_trace_audit_number', 'reference_number_diff', 'retrieval_nbr_diff', 'retrieval_ref_number', 'tt_original_reference_tranin') }} AS order_id,
        
        -- Currency and settlement logic
        CASE
            WHEN mybank_settlement_currency_array LIKE '%' || alpha_currency_code_diff || '%' THEN alpha_currency_code_diff
            ELSE COALESCE(
                lcot.settle_alpha_currency_code_tran_eod,
                CASE WHEN tt_transaction_amount_tranin IS NOT NULL THEN 'GBP' END
            )
        END AS alpha_currency_code,
        
        CASE WHEN payment_method = 'mpaynet' THEN 2 ELSE 4 END AS currency_exponent,
        
        -- Amount calculations
        CASE
            WHEN mybank_settlement_currency_array LIKE '%' || alpha_currency_code_diff || '%' THEN
                {{ safe_cast('transaction_amount_diff', 'NUMERIC') }} * xb_signed_rate
            ELSE
                {{ safe_cast('COALESCE(settlement_currency_amount_tran_eod, tt_transaction_amount_tranin)', 'NUMERIC') }} * xb_signed_rate
        END AS amount,
        
        -- Country code logic
        CASE
            WHEN dim_ctry.mybank_region IS NOT NULL THEN dim_ctry.country_code_2a
            ELSE 'GB'
        END AS country_code,
        
        -- Static values
        NULL AS reference_transaction_id,
        'PROCESSED' AS status,
        NULL AS reason_text,
        payment_method,
        {{ format_date_snowflake('file_date_diff', 'YYYYMMDD') }} AS processing_date,
        file_date_diff AS file_date,
        arn,
        
        -- Settlement currency and amount
        CASE
            WHEN mybank_settlement_currency_array LIKE '%' || alpha_currency_code_diff || '%' THEN alpha_currency_code_diff
            ELSE COALESCE(
                lcot.settle_alpha_currency_code_tran_eod,
                CASE WHEN tt_transaction_amount_tranin IS NOT NULL THEN 'GBP' END
            )
        END AS settlement_alpha_currency_code,
        
        CASE
            WHEN mybank_settlement_currency_array LIKE '%' || alpha_currency_code_diff || '%' THEN
                {{ safe_cast('transaction_amount_diff', 'NUMERIC') }} * xb_signed_rate
            ELSE
                {{ safe_cast('COALESCE(settlement_currency_amount_tran_eod, tt_transaction_amount_tranin)', 'NUMERIC') }} * xb_signed_rate
        END AS expected_settled_amount,
        
        CASE WHEN payment_method = 'mpaynet' THEN 2 ELSE 4 END AS settlement_currency_exponent,
        {{ format_date_snowflake('DATEADD(day, 1, file_date_diff)', 'YYYYMMDD') }} AS expected_settlement_date,
        1 AS scheme_rate,
        
        -- Card and charge type information
        card_type_diff AS card_type,
        card_type_desc,
        charge_type_diff AS charge_type,
        charge_type_desc,
        COALESCE(fee_program_ind, ird) AS interchange_fee_program,
        fee_program_desc AS interchange_fee_program_desc,
        CONCAT(CAST(xb_signed_rate * -100 AS STRING), '%') AS interchange_rate,
        
        -- Reference fields
        {{ format_system_trace_audit_number('sys_audit_nbr_diff', 'system_trace_audit_number', 'reference_number_diff') }} AS system_trace_audit_number,
        {{ format_retrieval_ref_number('retrieval_nbr_diff', 'retrieval_ref_number', 'tt_original_reference_tranin') }} AS retrieval_ref_number,
        
        -- Additional fields
        card_country_code,
        account_funding_source,
        card_bin,
        bin_detail_sk,
        
        -- Aggregate references (calculated after other fields are available)
        CONCAT(
            COALESCE({{ safe_cast(format_date_snowflake("DATEADD(day, 1, " ~ "file_date_diff" ~ ")", 'YYDDD'), 'STRING') }}, ''),
            COALESCE(geographical_region, ''),
            COALESCE(acquirer_bin, ''),
            COALESCE(merchant_number_diff, ''),
            '0008'
        ) AS merchant_aggregate_reference,
        CONCAT(
            COALESCE({{ safe_cast(format_date_snowflake("DATEADD(day, 1, " ~ "file_date_diff" ~ ")", 'YYDDD'), 'STRING') }}, ''),
            COALESCE(geographical_region, ''),
            COALESCE(acquirer_bin, ''),
            '0008'
        ) AS bin_aggregate_reference,
        
        -- Merchant sequence
        CONCAT(COALESCE(merchant_number_diff, ''), COALESCE({{ generate_uuid() }}, '')) AS merchant_sequence,
        
        -- Domestic/International classification
        {{ classify_domestic_international('country_code', 'alpha_currency_code', 'charge_type_diff') }} AS domestic_international_ind,
        
        -- Event timestamp
        CONCAT(REPLACE(SUBSTR(CAST(create_date_time AS STRING), 1, 19), ' ', 'T'), 'Z') AS event_timestamp
        
    FROM (
        -- Main data selection with complex joins
        SELECT DISTINCT
            '-1' AS chargeback_sk_chargeback_eod,
            merchant_number_diff,
            alpha_currency_code_diff,
            card_type_diff,
            charge_type_diff,
            transaction_amount_diff,
            transaction_id_diff,
            card_scheme_diff,
            CASE
                WHEN card_scheme_diff = 'mp' THEN 'mpaynet'
                WHEN card_scheme_diff = 'vp' THEN 'vpaynet'
            END AS payment_method,
            tt_acq_reference_number_tranin AS arn,
            moto_ec_indicator_diff,
            cardholder_id_method_diff,
            trans_sk_guid,
            corp_diff,
            region_diff,
            principal_diff,
            associate_diff,
            chain_diff,
            tran_code_diff,
            file_date_diff,
            tt_acq_reference_number_tranin,
            settle_alpha_currency_code_tran_eod,
            settlement_currency_amount_tran_eod,
            cross_border_indicator_diff,
            transaction_date_diff,
            reference_number_diff,
            tt_original_reference_tranin,
            dimipc.fee_program_ind,
            dimipc.ird,
            tt_transaction_amount_tranin,
            system_trace_audit_number,
            retrieval_ref_number,
            card_country_code,
            account_funding_source,
            card_bin,
            bin_detail_sk,
            dim_card_desc.card_type_desc,
            dim_charge_desc.charge_type_desc,
            dimipc.fee_program_desc,
            xb.xb_signed_rate,
            corp_guid,
            region_guid,
            sys_audit_nbr_diff,
            retrieval_nbr_diff
        FROM {{ ref('stg_temp_data_source') }} a
        INNER JOIN {{ ref('stg_xb_rate_matrix') }} xb
            ON a.corp_diff = xb.corp
            AND a.card_scheme_diff = xb.card_scheme
            AND ({{ array_contains('xb.region_inclusion_array', 'a.region_diff') }} OR {{ array_length('xb.region_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.region_exclusion_array', 'a.region_diff') }}
            AND (a.card_type_diff = xb.card_type_inclusion_regexp OR xb.card_type_inclusion_regexp IS NULL)
            AND (a.card_type_diff != xb.card_type_exclusion_regexp)
            AND ({{ regexp_contains('a.charge_type_diff', 'xb.charge_type_inclusion_regexp') }} OR COALESCE(TRIM(xb.charge_type_inclusion_regexp), '') = '')
            AND (NOT {{ regexp_contains('a.charge_type_diff', 'xb.charge_type_exclusion_regexp') }} OR COALESCE(TRIM(xb.charge_type_exclusion_regexp), '') = '') 
            AND ({{ array_contains('xb.currency_code_inclusion_array', 'a.alpha_currency_code_diff') }} OR {{ array_length('xb.currency_code_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.currency_code_exclusion_array', 'a.alpha_currency_code_diff') }}
            AND ({{ array_contains('xb.tran_code_inclusion_array', 'a.tran_code_diff') }} OR {{ array_length('xb.tran_code_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.tran_code_exclusion_array', 'a.tran_code_diff') }}
            AND ({{ array_contains('xb.xb_ind_inclusion_array', 'a.cross_border_indicator_diff') }} OR {{ array_length('xb.xb_ind_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.xb_ind_exclusion_array', 'a.cross_border_indicator_diff') }}
            AND ({{ array_contains('xb.moto_ind_inclusion_array', 'a.moto_ec_indicator_diff') }} OR {{ array_length('xb.moto_ind_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.moto_ind_exclusion_array', 'a.moto_ec_indicator_diff') }}
            AND ({{ array_contains('xb.chid_method_inclusion_array', 'a.cardholder_id_method_diff') }} OR {{ array_length('xb.chid_method_inclusion_array') }} = 0)
            AND NOT {{ array_contains('xb.chid_method_exclusion_array', 'a.cardholder_id_method_diff') }}
            AND (a.file_date_diff >= COALESCE(xb.effective_date, '1900-01-01') AND a.file_date_diff < COALESCE(xb.end_date, '2099-12-31'))
        LEFT JOIN {{ source('transformed_layer', 'dim_crd_typ') }} dim_card_desc
            ON a.card_type_diff = dim_card_desc.card_type
            AND dim_card_desc.current_ind = '0'
        LEFT JOIN {{ source('transformed_layer', 'dim_chrg_typ') }} dim_charge_desc
            ON a.corp_diff = dim_charge_desc.corporate
            AND a.charge_type_diff = dim_charge_desc.charge_type
            AND dim_charge_desc.current_ind = '0'
        LEFT JOIN (
            SELECT * EXCEPT (r)
            FROM (
                SELECT
                    dim.fee_program_ind,
                    dim.ird,
                    dim.card_type,
                    dim.charge_type,
                    dim.fee_program_desc,
                    dim.interchange_rate_percent,
                    dim.interchange_fee_amount,
                    ROW_NUMBER() OVER (PARTITION BY card_type, charge_type ORDER BY COALESCE(fee_program_ind, ird) DESC) AS r
                FROM {{ source('transformed_layer', 'dim_intchg_prgm_cmplnc') }} dim
            )
            WHERE r = 1
        ) dimipc
            ON a.card_type_diff = dimipc.card_type
            AND a.charge_type_diff = dimipc.charge_type
    ) lcot
    LEFT JOIN (
        SELECT DISTINCT
            country_code_2a,
            mybank_geographical_region,
            mybank_region
        FROM {{ source('transformed_layer', 'dim_iso_ctry_cd') }}
        WHERE mybank_corporate = '{{ var("mybank_corporate") }}'
        AND current_ind = 0
    ) dim_ctry
        ON lcot.region_diff = dim_ctry.mybank_region
    LEFT JOIN (
        SELECT DISTINCT
            mybank_settlement_currency_array,
            mybank_corporate,
            mybank_region
        FROM {{ source('transformed_layer', 'dim_iso_ctry_cd') }}
        WHERE mybank_corporate = '{{ var("mybank_corporate") }}'
        AND current_ind = 0
    ) dim_ctry1
        ON dim_ctry1.mybank_corporate = lcot.corp_guid
        AND lcot.region_guid = dim_ctry1.mybank_region
    LEFT JOIN (
        SELECT DISTINCT
            {{ left_pad('CAST(mp_ica AS STRING)', 6, '0') }} AS mp_ica,
            vpaynet_bin,
            region,
            ROW_NUMBER() OVER(PARTITION BY region ORDER BY mp_ica DESC, vpaynet_bin DESC, region DESC) AS rn
        FROM {{ source('transformed_layer', 'uk_mmbinsq_assc_parm') }}
        WHERE corporate = '{{ var("mybank_corporate") }}'
        AND current_ind = '0'
    ) assc_parm1
        ON lcot.region_diff = assc_parm1.region
        AND rn = 1
)

-- Final SELECT for the model
SELECT * FROM source_data

{% if is_incremental() %}
    -- When running incrementally, only process new or updated records
    WHERE etlbatchid > (SELECT MAX(etlbatchid) FROM {{ this }})
{% endif %}