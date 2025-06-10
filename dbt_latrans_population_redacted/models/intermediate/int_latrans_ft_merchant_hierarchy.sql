{{
  config(
    materialized='view',
    description='Intermediate model for merchant information join and hierarchy dimension lookups'
  )
}}

{#- 
  This intermediate model replicates the TransFT processing logic:
  1. Join with merchant information dimension
  2. Perform hierarchy dimension lookups (corporate, region, principal, associate, chain)
  This combines the logic from TransFTJoins.joinMerchantInformationDetails() 
  and TempTransFT0Processing.expand() methods
-#}

with source_transactions as (
  select * from {{ ref('stg_latrans_ft_source') }}
),

-- Merchant information dimension join
merchant_info_joined as (
  select
    t.*,
    coalesce(m.merchant_information_sk, {{ var('dmx_lookup_failure') }}) as merchant_information_sk,
    coalesce(m.hierarchy, '') as mhd_hierarchy,
    coalesce(m.corporate, '') as mhd_corporate,
    coalesce(m.region, '') as mhd_region
  from source_transactions t
  left join {{ ref('stg_dim_merchant_info') }} m
    on t.merchant_number_int = m.merchant_number
),

-- Corporate dimension lookup
corporate_joined as (
  select
    t.*,
    coalesce(c.corporate_sk, {{ var('dmx_lookup_failure') }}) as corporate_sk
  from merchant_info_joined t
  left join {{ ref('stg_dim_corporate') }} c
    on t.corporate = c.corporate
),

-- Region dimension lookup
region_joined as (
  select
    t.*,
    coalesce(r.region_sk, {{ var('dmx_lookup_failure') }}) as region_sk
  from corporate_joined t
  left join {{ ref('stg_dim_region') }} r
    on t.corporate_sk = r.corporate_sk
    and t.region = r.region
),

-- Principal dimension lookup
principal_joined as (
  select
    t.*,
    coalesce(p.principal_sk, {{ var('dmx_lookup_failure') }}) as principal_sk
  from region_joined t
  left join {{ ref('stg_dim_principal') }} p
    on t.corporate_sk = p.corporate_sk
    and t.region_sk = p.region_sk
    and t.principal = p.principal
),

-- Associate dimension lookup
associate_joined as (
  select
    t.*,
    coalesce(a.associate_sk, {{ var('dmx_lookup_failure') }}) as associate_sk
  from principal_joined t
  left join {{ ref('stg_dim_associate') }} a
    on t.corporate_sk = a.corporate_sk
    and t.region_sk = a.region_sk
    and t.principal_sk = a.principal_sk
    and t.associate = a.associate
),

-- Chain dimension lookup
chain_joined as (
  select
    t.*,
    coalesce(c.chain_sk, {{ var('dmx_lookup_failure') }}) as chain_sk
  from associate_joined t
  left join {{ ref('stg_dim_chain') }} c
    on t.corporate_sk = c.corporate_sk
    and t.region_sk = c.region_sk
    and t.principal_sk = c.principal_sk
    and t.associate_sk = c.associate_sk
    and t.chain = c.chain
)

select * from chain_joined

{#- 
  This model outputs transaction data enriched with:
  - Merchant information surrogate key
  - All hierarchy dimension surrogate keys (corporate_sk, region_sk, principal_sk, associate_sk, chain_sk)
  - Merchant hierarchy details for reference
  
  Failed lookups are marked with the DMX_LOOKUP_FAILURE constant (-2)
-#}