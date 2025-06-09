{{
    config(
        materialized='table',
        schema='marts',
        post_hook="UPDATE {{ source('configuration', 'max_etl_config') }} SET max_etlbatchid = (SELECT COALESCE(MAX(etlbatchid), {{ var('source_etlbatchid') }}) FROM {{ ref('int_youcm_gppn_temp') }}) WHERE process = 'GPN'"
    )
}}

-- Model to handle max etlbatchid updates using post-hook
-- Equivalent to get_max_etlbatchid UPDATE operation in the original script

SELECT
    'GPN' AS process,
    MAX(etlbatchid) AS max_etlbatchid_calculated,
    {{ var('source_etlbatchid') }} AS source_etlbatchid
FROM {{ ref('int_youcm_gppn_temp') }}