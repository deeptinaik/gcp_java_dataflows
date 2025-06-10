-- Test to validate data lineage integrity between staging and mart models
-- Ensures record counts match between trusted and transformed layers

select 
    'staging' as layer,
    count(*) as record_count
from {{ ref('stg_tds_batch') }}

union all

select 
    'marts' as layer,
    count(*) as record_count
from {{ ref('tds_batch_transformed') }}

having count(distinct record_count) > 1