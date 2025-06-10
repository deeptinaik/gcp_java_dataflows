{{
  config(
    materialized='table',
    description='Final LA Trans FT mart table - fully processed transaction data for transformed layer',
    unique_key='la_trans_ft_uuid',
    partition_by={
      'field': 'transaction_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['corporate_sk', 'region_sk', 'merchant_information_sk']
  )
}}

{#- 
  This mart model represents the final output of the LA Trans FT pipeline
  It writes to transformed_layer.la_trans_ft table and includes all final transformations,
  data cleaning, and validation that were performed in the Java pipeline
  Replicates the TransFTFinalFilter and final output logic
-#}

with settlement_processed_data as (
  select * from {{ ref('int_latrans_ft_settlement_currency_processing') }}
),

-- Final data cleaning and validation (replicates CleaningData and CleaningSettlementData)
cleaned_data as (
  select
    *,
    -- Data quality flags and corrections
    case 
      when transaction_amount is null or transaction_amount = ''
      then '0'
      else transaction_amount
    end as cleaned_transaction_amount,
    
    case 
      when settled_amount is null or settled_amount = ''
      then '0'
      else settled_amount
    end as cleaned_settled_amount,
    
    -- Ensure currency codes are properly set
    coalesce(alpha_currency_code, '') as final_alpha_currency_code,
    coalesce(settle_alpha_currency_code, '') as final_settle_alpha_currency_code,
    
    -- Date validation and formatting
    case 
      when transaction_date = '' or transaction_date is null
      then null
      else safe.parse_date('%Y%m%d', transaction_date)
    end as parsed_transaction_date,
    
    case 
      when deposit_date = '' or deposit_date is null
      then null
      else safe.parse_date('%Y%m%d', deposit_date)
    end as parsed_deposit_date,
    
    case 
      when authorization_date = '' or authorization_date is null
      then null
      else safe.parse_date('%Y%m%d', authorization_date)
    end as parsed_authorization_date
    
  from settlement_processed_data
),

-- Final field selection and mapping (replicates Utility.getTransFT0Fields())
final_output as (
  select
    -- Primary business keys
    la_trans_ft_uuid,
    
    -- ETL control fields
    etl_batch_date,
    current_timestamp() as create_date_time,
    current_timestamp() as update_date_time,
    
    -- Hierarchy and merchant fields
    hierarchy,
    corporate,
    region,
    principal,
    associate,
    chain,
    merchant_number,
    merchant_number_int,
    merchant_information_sk,
    
    -- Dimension surrogate keys
    corporate_sk,
    region_sk,
    principal_sk,
    associate_sk,
    chain_sk,
    
    -- Transaction core fields
    parsed_transaction_date as transaction_date,
    transaction_time,
    transaction_identifier,
    cleaned_transaction_amount as transaction_amount,
    
    -- Currency fields
    final_alpha_currency_code as alpha_currency_code,
    iso_numeric_currency_code,
    transaction_currency_code_sk,
    
    -- Settlement fields
    cleaned_settled_amount as settled_amount,
    final_settle_alpha_currency_code as settle_alpha_currency_code,
    settle_iso_currency_code,
    
    -- Authorization fields
    authorization_amount,
    supplemental_authorization_amount,
    authorization_code,
    parsed_authorization_date as authorization_date,
    
    -- Additional transaction fields
    card_type,
    charge_type,
    terminal_number,
    card_acceptor_id,
    reference_number,
    batch_control_number,
    cash_letter_number,
    
    -- Merchant and location fields
    merchant_dba_name,
    
    -- Date fields
    file_date,
    parsed_deposit_date as deposit_date,
    file_time,
    
    -- Additional fields
    trans_id,
    trans_source,
    
    -- Lodging fields (if present)
    lodging_check_in_date,
    car_rental_check_out_date
    
  from cleaned_data
  
  -- Final data quality filter (replicates TransFTFinalFilter logic)
  where 
    -- Ensure we have valid core transaction data
    merchant_number_int is not null
    and transaction_amount is not null
    and parsed_transaction_date is not null
    
    -- Exclude test or invalid transactions
    and not (
      merchant_number_int = '0' 
      or merchant_number_int = ''
      or merchant_number_int is null
    )
)

select * from final_output

{#- 
  This final mart represents the complete LA Trans FT processing pipeline output.
  It includes:
  - All field transformations from the Java FieldTransformation class
  - Merchant information and hierarchy dimension joins
  - Currency code processing and ISO numeric lookups
  - Settlement currency processing
  - Data cleaning and validation
  - Final filtering and quality checks
  
  The data is partitioned by transaction_date for performance and 
  clustered by key dimension fields for efficient querying.
-#}