{{
  config(
    materialized='view',
    schema='trusted_layer'
  )
}}

-- Staging model for TDS Batch data (trusted layer)
-- This model represents the trusted_layer.first_table from the mapping document
-- Applies direct pull transformations and ETL-generated fields

with source_data as (
    select * from {{ source('tds_raw', 'tds_batch') }}
),

transformed as (
    select
        -- ETL Generated fields
        {{ generate_etl_batch_id() }} as etlbatchid,
        {{ generate_etl_batch_date() }} as etl_batch_date,
        
        -- Direct pull from source with DW_CREATE_DTM mapping
        dw_create_dtm as dw_create_datetime,
        
        -- Direct pull transformations from TDS_BATCH source
        batch_nbr,
        device_nbr,
        status,
        date_closed,
        ext_batch_nbr,
        capture_method,
        total_amount,
        total_count,
        open_amount,
        eft_bal_ind,
        userid,
        cust_nbr,
        cutoff_time,
        eod_flag,
        settle_id_matrix,
        card_type_matrix,
        orig_batch_nbr,
        purchase_count,
        
        -- Audit fields
        {{ generate_current_timestamp() }} as dbt_load_timestamp,
        {{ generate_current_timestamp() }} as dbt_update_timestamp
        
    from source_data
)

select * from transformed