{{
  config(
    materialized='view',
    description='Staging view for default currency dimension data'
  )
}}

{#- 
  This staging model prepares the default currency dimension data
  Replicates the DIM_DEFAULT_CURRENCY_QUERY from CommonUtil.java line 238-239
-#}

select distinct
  {{ create_corporate_region_key('corporate', 'region') }} as ddc_corporate_region,
  currency_code as ddc_currency_code
from {{ source('transformed_layer', 'dim_default_curr') }}

{#- 
  Note: The original Java query created a composite key by concatenating
  corporate and region as strings, which is replicated here
-#}