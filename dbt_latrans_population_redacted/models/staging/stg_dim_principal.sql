{{
  config(
    materialized='view',
    description='Staging view for principal dimension data'
  )
}}

select
  corporate_sk,
  region_sk,
  principal,
  principal_sk
from {{ source('transformed_layer', 'dim_principal') }}