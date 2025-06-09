{{
    config(
        materialized='table',
        schema='staging'
    )
}}

-- Staging model equivalent to load_temp_table in BigQuery script
-- Filters dimension_merch_info data based on etlbatchid parameter

SELECT 
    *
FROM {{ source('cnsmp_layer', 'dimension_merch_info') }}
WHERE 
    current_ind = '0'
    AND purge_flag = 'N'
    AND etlbatchid > {{ var('source_etlbatchid') }}