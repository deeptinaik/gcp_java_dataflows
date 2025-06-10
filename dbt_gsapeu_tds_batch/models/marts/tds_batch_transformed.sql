{{
  config(
    materialized='table',
    schema='transformed_layer',
    pre_hook="TRUNCATE TABLE {{ this }}",
    cluster_by=['cust_nbr', 'batch_nbr', 'status'],
    partition_by={
      "field": "etl_batch_date",
      "data_type": "date"
    }
  )
}}

-- Main transformation model for TDS Batch data (transformed layer)
-- This model represents the transformed_layer.second_table from the mapping document
-- Applies data type conversions and enhanced transformations

with staged_data as (
    select * from {{ ref('stg_tds_batch') }}
),

final as (
    select
        -- ETL Generated fields with type conversions
        {{ safe_cast('etlbatchid', 'INTEGER') }} as etlbatchid,
        etl_batch_date,
        
        -- Direct pull with TIMESTAMP conversion
        {{ safe_cast('dw_create_datetime', 'TIMESTAMP') }} as dw_create_datetime,
        
        -- ETL generated update timestamp (new field for transformed layer)
        {{ generate_current_timestamp() }} as dw_update_datetime,
        
        -- Direct pull transformations with data type conversions
        {{ safe_cast('batch_nbr', 'INTEGER') }} as batch_nbr,
        {{ safe_cast('device_nbr', 'NUMERIC') }} as device_nbr,
        status, -- STRING remains STRING
        {{ safe_cast('date_closed', 'DATE') }} as date_closed,
        {{ safe_cast('ext_batch_nbr', 'NUMERIC') }} as ext_batch_nbr,
        capture_method, -- STRING remains STRING
        {{ safe_cast('total_amount', 'NUMERIC') }} as total_amount,
        {{ safe_cast('total_count', 'NUMERIC') }} as total_count,
        {{ safe_cast('open_amount', 'NUMERIC') }} as open_amount,
        {{ safe_cast('eft_bal_ind', 'NUMERIC') }} as eft_bal_ind,
        userid, -- STRING remains STRING
        {{ safe_cast('cust_nbr', 'NUMERIC') }} as cust_nbr,
        {{ safe_cast('cutoff_time', 'DATE') }} as cutoff_time,
        {{ safe_cast('eod_flag', 'NUMERIC') }} as eod_flag,
        settle_id_matrix, -- STRING remains STRING
        card_type_matrix, -- STRING remains STRING
        {{ safe_cast('orig_batch_nbr', 'NUMERIC') }} as orig_batch_nbr,
        {{ safe_cast('purchase_count', 'NUMERIC') }} as purchase_count,
        
        -- Audit fields
        dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from staged_data
)

select * from final