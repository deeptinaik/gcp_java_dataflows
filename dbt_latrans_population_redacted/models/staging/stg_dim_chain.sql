{{
  config(
    materialized='view',
    description='Staging view for chain dimension data'
  )
}}

select
  corporate_sk,
  region_sk,
  principal_sk,
  associate_sk,
  chain,
  chain_sk
from {{ source('transformed_layer', 'dim_chain') }}