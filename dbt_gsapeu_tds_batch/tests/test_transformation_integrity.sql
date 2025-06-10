-- Test to validate data type transformations are applied correctly
-- Ensures that numeric conversions don't result in unexpected nulls

with transformation_validation as (
    select 
        stg.batch_nbr as stg_batch_nbr,
        mart.batch_nbr as mart_batch_nbr,
        stg.cust_nbr as stg_cust_nbr,
        mart.cust_nbr as mart_cust_nbr,
        stg.etlbatchid as stg_etlbatchid,
        mart.etlbatchid as mart_etlbatchid
    from {{ ref('stg_tds_batch') }} stg
    join {{ ref('tds_batch_transformed') }} mart
        on stg.batch_nbr = mart.batch_nbr::string
)

select *
from transformation_validation
where 
    (stg_batch_nbr is not null and mart_batch_nbr is null)
    or (stg_cust_nbr is not null and mart_cust_nbr is null)
    or (stg_etlbatchid is not null and mart_etlbatchid is null)