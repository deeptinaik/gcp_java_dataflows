{{
    config(
        materialized='table',
        schema='marts',
        post_hook="UPDATE {{ this }} SET purge_flag = 'Y' WHERE merchant_number IN (SELECT merchant_number FROM {{ source('cnsmp_layer', 'dimension_merch_info') }} WHERE purge_flag = 'Y') AND acquirer_name = 'GPN'"
    )
}}

-- Model to handle purge flag updates using post-hook
-- Equivalent to query_2 and query_3 in the original script

SELECT
    merchant_number,
    acquirer_name,
    purge_flag,
    'Y' AS new_purge_flag_y,
    'N' AS new_purge_flag_n
FROM {{ ref('master_merch_info') }}
WHERE acquirer_name = 'GPN'