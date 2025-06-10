{{ config(
    materialized='view',
    schema='staging_layer'
) }}

-- Staging model for cross-border rate matrix
SELECT 
    corp,
    card_scheme,
    region_inclusion_array,
    region_exclusion_array,
    card_type_inclusion_regexp,
    card_type_exclusion_regexp,
    charge_type_inclusion_regexp,
    charge_type_exclusion_regexp,
    currency_code_inclusion_array,
    currency_code_exclusion_array,
    tran_code_inclusion_array,
    tran_code_exclusion_array,
    xb_ind_inclusion_array,
    xb_ind_exclusion_array,
    moto_ind_inclusion_array,
    moto_ind_exclusion_array,
    chid_method_inclusion_array,
    chid_method_exclusion_array,
    effective_date,
    end_date,
    xb_signed_rate
FROM {{ source('transformed_layer', 'dim_xb_rt_mtx_mybank') }}