-- Test for duplicate transactions based on business key
-- Replicates the uniqueness that would be enforced by the hash key in Java
select 
  merchant_number_int,
  transaction_date,
  transaction_time,
  transaction_amount,
  reference_number,
  count(*) as duplicate_count
from {{ ref('mart_latrans_ft') }}
group by 
  merchant_number_int,
  transaction_date,
  transaction_time,
  transaction_amount,
  reference_number
having count(*) > 1