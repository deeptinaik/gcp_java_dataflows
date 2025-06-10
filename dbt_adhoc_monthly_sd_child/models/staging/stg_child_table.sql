{{
  config(
    materialized='view',
    description='Staging view for child table data - represents the source table read by the original Dataflow pipeline'
  )
}}

{#- 
  This staging model represents the source child table that was read using:
  BigQueryIO.readTableRows().from(options.getChildTableDescription())
  
  For demonstration purposes, we're using seed data.
  In production, this would reference the actual source table using the variable:
  from {{ source('raw_data', 'child_table') }}
  
  Or with runtime parameter: 
  from {{ var('child_table_name', ref('sample_child_table_data')) }}
-#}

select
  -- Primary business fields (example structure)
  merchant_number,
  child_record_id,
  record_type,
  
  -- Soft delete control fields (key fields from LiteralConstant.java)
  current_ind,
  dw_update_date_time,
  
  -- Additional audit fields
  created_date,
  created_by,
  
  -- Sample additional fields that might exist in a child table
  parent_id,
  status,
  description,
  amount,
  currency_code
  
from {{ var('child_table_name', ref('sample_child_table_data')) }}