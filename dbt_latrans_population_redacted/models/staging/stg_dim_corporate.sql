{{
  config(
    materialized='view',
    description='Staging views for hierarchy dimension tables (corporate, region, principal, associate, chain)'
  )
}}

{#- 
  These staging models prepare the hierarchy dimension data
  Used in TempTransFT0Processing for dimension lookups
-#}

-- Corporate dimension
select
  corporate,
  corporate_sk
from {{ source('transformed_layer', 'dim_corporate') }}

{#- End of corporate staging model -#}