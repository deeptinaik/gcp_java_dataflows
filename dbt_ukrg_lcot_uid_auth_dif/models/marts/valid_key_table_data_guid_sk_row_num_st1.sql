{{
    config(
        materialized='incremental',
        schema='backup_xl_layer',
        unique_key=['north_uk_auth_fin_sk', 'trans_sk'],
        merge_update_columns=['joinind_auth_dif', 'joininddate_auth_dif']
    )
}}

-- Simple test version to isolate the compilation error

SELECT 
    'test' AS test_field,
    {{ var('etl_batch_id') }} AS etlbatchid,
    '{{ var("default_joinind_date") }}' AS joininddate

FROM {{ ref('stg_temp_uk_auth_table') }}
WHERE 1=1
    AND {{ auth_response_code_valid('resp_code_num_gnap_auth', 'record_type_gnap_auth', 'rec_type_gnap_auth', 'excep_reason_code_gnap_auth') }}
LIMIT 10
