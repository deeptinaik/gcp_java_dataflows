{{
  config(
    materialized='view',
    description='Staging view for ISO numeric currency code dimension data'
  )
}}

{#- 
  This staging model prepares the ISO numeric currency code dimension data
  Replicates the DIM_ISO_NUMERIC_CURRENCY_CODE_QUERY from CommonUtil.java line 240-241
-#}

select distinct
  currency_code as dincc_currency_code,
  iso_numeric_currency_code as dincc_iso_numeric_currency_code,
  iso_numeric_currency_code_sk as dincc_iso_numeric_currency_code_sk
from {{ source('transformed_layer', 'dim_iso_numeric_curr_code') }}

{#- 
  This dimension table provides mappings between:
  - Alpha currency codes (dincc_currency_code)
  - Numeric currency codes (dincc_iso_numeric_currency_code)
  - Surrogate keys (dincc_iso_numeric_currency_code_sk)
  
  Used for both alpha and numeric currency code lookups in the pipeline
-#}