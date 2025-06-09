-- Custom test to validate merchant number consistency across models
SELECT 
    merchant_number
FROM {{ ref('stg_dimension_merch_info_temp') }}
EXCEPT
SELECT 
    merchant_number
FROM {{ ref('int_youcm_gppn_temp') }}