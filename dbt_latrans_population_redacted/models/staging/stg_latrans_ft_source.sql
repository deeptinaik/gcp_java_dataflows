{{
  config(
    materialized='view',
    description='Staging view for LA Trans FT source data with field transformations applied'
  )
}}

{#- 
  This staging model replicates the FieldTransformation.java logic
  It reads from trusted_layer.la_trans_ft and applies all field-level transformations
  that were performed in the Java DoFn processElement method
-#}

with source_data as (
  select * from {{ source('trusted_layer', 'la_trans_ft') }}
),

transformed_fields as (
  select
    -- Direct field mappings (no transformation required)
    etlbatchid as etl_batch_date,
    
    -- Hierarchy construction (replicates Java StringBuilder logic)
    concat(
      coalesce(cast(corp as string), ''), ',',
      coalesce(cast(region as string), ''), ',',
      coalesce(cast(principal as string), ''), ',',
      coalesce(cast(associate as string), ''), ',',
      coalesce(cast(chain as string), '')
    ) as hierarchy,
    
    corp as corporate,
    region,
    principal,
    associate,
    chain,
    merchant_number,
    
    -- Merchant number integer conversion with leading zero trimming
    {{ trim_leading_zeros_merchant_number('merchant_number') }} as merchant_number_int,
    
    -- Date transformations
    {{ transform_date_mmddyy_to_yyyymmdd('transaction_date') }} as transaction_date,
    {{ transform_date_mmddyy_to_yyyymmdd('file_date_original') }} as file_date_original,
    {{ transform_date_yymmdd_to_yyyymmdd('file_date') }} as file_date,
    {{ transform_date_yymmdd_to_yyyymmdd('deposit_date') }} as deposit_date,
    {{ transform_lodging_car_rental_date('lodging_checkin_date') }} as lodging_check_in_date,
    {{ transform_lodging_car_rental_date('car_rental_checkout_date') }} as car_rental_check_out_date,
    {{ transform_authorization_date('transaction_date', 'authorization_date') }} as authorization_date,
    
    -- Time transformations
    {{ get_valid_time('transaction_time') }} as transaction_time,
    {{ get_valid_time('file_time') }} as file_time,
    
    -- Amount transformations (using exponent from variables)
    {{ transform_amount_with_scale('transaction_amount_new', var('amount_exponent')) }} as transaction_amount,
    {{ transform_amount_with_scale('settled_amount', var('amount_exponent')) }} as settled_amount,
    {{ transform_authorization_amount('authorization_amount') }} as authorization_amount,
    {{ transform_authorization_amount('supplemental_authorization_amount') }} as supplemental_authorization_amount,
    
    -- Transaction identifier cleaning
    {{ clean_transaction_identifier('transaction_id') }} as transaction_identifier,
    
    -- Batch control number creation
    {{ create_batch_control_number('deposit_date', 'cash_letter_number') }} as batch_control_number,
    
    -- Currency code fields
    currency_code as transaction_currency_code,
    settle_currency_code as settled_currency_code,
    
    -- Pass through other important fields
    cash_letter_number,
    card_acceptor_id,
    terminal_number,
    merchant_dba_name,
    authorization_code,
    reference_number,
    card_type,
    charge_type,
    trans_id,
    trans_source,
    
    -- Source tracking fields
    create_date_time,
    update_date_time,
    
    -- Generate UUID for unique identification (replacing hash key logic)
    generate_uuid() as la_trans_ft_uuid
    
  from source_data
)

select * from transformed_fields

{#- 
  This model represents the first stage of the pipeline transformation,
  equivalent to the output of FieldTransformation.processElement() method.
  All subsequent transformations will build upon this staged data.
-#}