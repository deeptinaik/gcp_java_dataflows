{{
  config(
    materialized='view',
    description='Staging view for merchant information dimension data'
  )
}}

{#- 
  This staging model prepares the merchant information dimension data
  for joining with the main transaction data.
  Replicates the DIM_MERCHANT_INFORMATION_QUERY from CommonUtil.java
-#}

select
  merchant_number,
  hierarchy,
  corporate,
  region,
  merchant_information_sk
from {{ source('transformed_layer', 'dim_merchant_info') }}
where current_ind = '0'

{#- 
  Note: The original Java query used current_ind='0' which indicates
  active records in this dimension table
-#}