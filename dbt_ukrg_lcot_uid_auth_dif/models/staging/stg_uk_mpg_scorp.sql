{{
    config(
        materialized='view',
        schema='backup_xl_layer'
    )
}}

-- UK MPG SCORP configuration data
-- Converted from lcot_query_1_1 in ukrg_lcot_uid_auth_dif.sh

SELECT DISTINCT 
    corporate
FROM xl_layer.lcot_config_details 
WHERE 
    application_name = 'eu_mpg_s' 
    AND current_ind = 0