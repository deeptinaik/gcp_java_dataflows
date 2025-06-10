{{
  config(
    materialized='view',
    description='Staging view for region dimension data'
  )
}}

select
  corporate_sk,
  region,
  region_sk
from {{ source('transformed_layer', 'dim_region') }}