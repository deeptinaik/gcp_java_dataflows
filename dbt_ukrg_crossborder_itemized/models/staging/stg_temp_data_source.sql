{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for temp data source UK GNP supplemental table
SELECT 
    merchant_number_diff,
    alpha_currency_code_diff,
    card_type_diff,
    charge_type_diff,
    transaction_amount_diff,
    transaction_id_diff,
    card_scheme_diff,
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
    tt_transaction_amount_tranin,
    system_trace_audit_number,
    retrieval_ref_number,
    card_country_code,
    account_funding_source,
    card_bin,
    bin_detail_sk,
    corp_guid,
    region_guid,
    sys_audit_nbr_diff,
    retrieval_nbr_diff,
    moto_ec_indicator_diff,
    cardholder_id_method_diff
FROM {{ source('transformed_layer_commplat', 'temp_data_source_uk_gnp_sup_tbl_ltr') }}