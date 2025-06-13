{{
  config(
    materialized='view',
    description='Intermediate model for settlement currency code processing and validation'
  )
}}

{#- 
  This intermediate model replicates the settlement currency code processing logic:
  Similar to transaction currency processing but for settlement currency
  Replicates SplittingSettlementCurrencyCodeInvalidValid, 
  FilterSettlementCurrencyCodeByAlphaAndNumeric, and related transformations
-#}

with currency_processed_data as (
  select * from {{ ref('int_latrans_ft_currency_processing') }}
),

-- Step 1: Classify settlement currency codes and determine if default lookup needed
settlement_currency_validation as (
  select
    *,
    {{ classify_settlement_currency_code('settled_currency_code') }} as settle_currency_code_type,
    case 
      when {{ is_null_blank_currency_code('settled_currency_code') }}
      then true
      else false
    end as needs_default_settle_currency_lookup
  from currency_processed_data
),

-- Step 2: Default currency lookup for invalid settlement currency codes
-- Note: Using same merchant info for default lookup as transaction currency
default_settle_currency_lookup as (
  select
    t.*,
    case 
      when t.needs_default_settle_currency_lookup and d.ddc_currency_code is not null
      then d.ddc_currency_code
      else t.settled_currency_code
    end as updated_settle_currency_code
  from settlement_currency_validation t
  left join {{ ref('stg_dim_default_currency') }} d
    on t.needs_default_settle_currency_lookup = true
    and t.corporate_region_key = d.ddc_corporate_region
),

-- Step 3: Reclassify updated settlement currency codes
updated_settle_currency_classification as (
  select
    *,
    {{ classify_currency_code('updated_settle_currency_code') }} as updated_settle_currency_code_type
  from default_settle_currency_lookup
),

-- Step 4: Alpha settlement currency code lookups
alpha_settle_currency_joined as (
  select
    t.*,
    case 
      when t.updated_settle_currency_code_type = 'alpha'
      then coalesce(c.dincc_currency_code, t.updated_settle_currency_code)
      else t.updated_settle_currency_code
    end as settle_alpha_currency_code,
    case 
      when t.updated_settle_currency_code_type = 'alpha'
      then c.dincc_iso_numeric_currency_code
      else null
    end as settle_iso_currency_code
  from updated_settle_currency_classification t
  left join {{ ref('stg_dim_iso_numeric_currency_code') }} c
    on t.updated_settle_currency_code_type = 'alpha'
    and t.updated_settle_currency_code = c.dincc_currency_code
),

-- Step 5: Numeric settlement currency code lookups
final_settle_currency_joined as (
  select
    t.*,
    case 
      when t.updated_settle_currency_code_type = 'numeric' and c.dincc_currency_code is not null
      then c.dincc_currency_code
      when t.updated_settle_currency_code_type = 'alpha'
      then t.settle_alpha_currency_code
      else t.updated_settle_currency_code
    end as final_settle_alpha_currency_code,
    case 
      when t.updated_settle_currency_code_type = 'numeric' and c.dincc_iso_numeric_currency_code is not null
      then c.dincc_iso_numeric_currency_code
      when t.updated_settle_currency_code_type = 'alpha'
      then t.settle_iso_currency_code
      else null
    end as final_settle_iso_currency_code
  from alpha_settle_currency_joined t
  left join {{ ref('stg_dim_iso_numeric_currency_code') }} c
    on t.updated_settle_currency_code_type = 'numeric'
    and t.updated_settle_currency_code = c.dincc_iso_numeric_currency_code
)

select 
  *,
  -- Clean final settlement fields for output
  final_settle_alpha_currency_code as settle_alpha_currency_code,
  final_settle_iso_currency_code as settle_iso_currency_code
from final_settle_currency_joined

{#- 
  This model outputs transaction data with fully processed settlement currency codes:
  - settle_alpha_currency_code: The resolved settlement alpha currency code
  - settle_iso_currency_code: The corresponding settlement ISO numeric code
  - All original transaction, hierarchy, and currency data
-#}