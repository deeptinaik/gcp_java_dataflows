{{
  config(
    materialized='view',
    description='Intermediate model for transaction currency code processing and validation'
  )
}}

{#- 
  This intermediate model replicates the currency code processing logic:
  1. Split records into valid/invalid currency codes
  2. For invalid currency codes, lookup default currency from merchant info
  3. Classify currency codes as alpha, numeric, or null
  4. Join with ISO numeric currency code dimension
  This combines logic from SplittingCurrenCodeInvalidValid, LeftOuterJoinSideInput, 
  and FilterCurrencyCodeByAlphaAndNumeric transformations
-#}

with merchant_enriched_data as (
  select * from {{ ref('int_latrans_ft_merchant_hierarchy') }}
),

-- Step 1: Classify currency codes and determine if default lookup needed
currency_validation as (
  select
    *,
    {{ classify_currency_code('transaction_currency_code') }} as currency_code_type,
    case 
      when {{ is_null_blank_currency_code('transaction_currency_code') }}
      then true
      else false
    end as needs_default_currency_lookup,
    
    -- Create corporate_region key for default currency lookup
    {{ create_corporate_region_key('mhd_corporate', 'mhd_region') }} as corporate_region_key
  from merchant_enriched_data
),

-- Step 2: Default currency lookup for invalid currency codes
default_currency_lookup as (
  select
    t.*,
    case 
      when t.needs_default_currency_lookup and d.ddc_currency_code is not null
      then d.ddc_currency_code
      else t.transaction_currency_code
    end as updated_currency_code
  from currency_validation t
  left join {{ ref('stg_dim_default_currency') }} d
    on t.needs_default_currency_lookup = true
    and t.corporate_region_key = d.ddc_corporate_region
),

-- Step 3: Reclassify updated currency codes
updated_currency_classification as (
  select
    *,
    {{ classify_currency_code('updated_currency_code') }} as updated_currency_code_type
  from default_currency_lookup
),

-- Step 4: Alpha currency code lookups
alpha_currency_joined as (
  select
    t.*,
    case 
      when t.updated_currency_code_type = 'alpha'
      then coalesce(c.dincc_currency_code, t.updated_currency_code)
      else t.updated_currency_code
    end as alpha_currency_code,
    case 
      when t.updated_currency_code_type = 'alpha'
      then c.dincc_iso_numeric_currency_code
      else null
    end as iso_numeric_currency_code,
    case 
      when t.updated_currency_code_type = 'alpha'
      then c.dincc_iso_numeric_currency_code_sk
      else null
    end as transaction_currency_code_sk
  from updated_currency_classification t
  left join {{ ref('stg_dim_iso_numeric_currency_code') }} c
    on t.updated_currency_code_type = 'alpha'
    and t.updated_currency_code = c.dincc_currency_code
),

-- Step 5: Numeric currency code lookups
final_currency_joined as (
  select
    t.*,
    case 
      when t.updated_currency_code_type = 'numeric' and c.dincc_currency_code is not null
      then c.dincc_currency_code
      when t.updated_currency_code_type = 'alpha'
      then t.alpha_currency_code
      else t.updated_currency_code
    end as final_alpha_currency_code,
    case 
      when t.updated_currency_code_type = 'numeric' and c.dincc_iso_numeric_currency_code is not null
      then c.dincc_iso_numeric_currency_code
      when t.updated_currency_code_type = 'alpha'
      then t.iso_numeric_currency_code
      else null
    end as final_iso_numeric_currency_code,
    case 
      when t.updated_currency_code_type = 'numeric' and c.dincc_iso_numeric_currency_code_sk is not null
      then c.dincc_iso_numeric_currency_code_sk
      when t.updated_currency_code_type = 'alpha'
      then t.transaction_currency_code_sk
      else null
    end as final_transaction_currency_code_sk
  from alpha_currency_joined t
  left join {{ ref('stg_dim_iso_numeric_currency_code') }} c
    on t.updated_currency_code_type = 'numeric'
    and t.updated_currency_code = c.dincc_iso_numeric_currency_code
)

select 
  *,
  -- Clean final fields for output
  final_alpha_currency_code as alpha_currency_code,
  final_iso_numeric_currency_code as iso_numeric_currency_code,
  coalesce(final_transaction_currency_code_sk, {{ var('dmx_lookup_failure') }}) as transaction_currency_code_sk
from final_currency_joined

{#- 
  This model outputs transaction data with fully processed currency codes:
  - alpha_currency_code: The resolved alpha currency code
  - iso_numeric_currency_code: The corresponding ISO numeric code
  - transaction_currency_code_sk: The surrogate key for the currency
  - All original transaction and hierarchy data
-#}