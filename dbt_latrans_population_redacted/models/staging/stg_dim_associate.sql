{{
  config(
    materialized='view',
    description='Staging view for associate dimension data'
  )
}}

select
  corporate_sk,
  region_sk,
  principal_sk,
  associate,
  associate_sk
from {{ source('transformed_layer', 'dim_associate') }}