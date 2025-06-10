-- Test for data quality issues that would cause processing failures
-- Validates critical business rules from the original Java pipeline

-- Test for invalid currency codes that weren't resolved
select 
  la_trans_ft_uuid,
  alpha_currency_code,
  transaction_currency_code_sk
from {{ ref('mart_latrans_ft') }}
where 
  (alpha_currency_code is null or alpha_currency_code = '')
  and transaction_currency_code_sk = '{{ var("dmx_lookup_failure") }}'