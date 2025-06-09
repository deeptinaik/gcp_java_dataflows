{#-
  Test to validate data lineage and integrity between source and transformed data
  Ensures no records are lost during transformation and all business rules are maintained
-#}

with source_count as (
  select count(*) as source_records
  from {{ ref('stg_child_table') }}
),

target_count as (
  select count(*) as target_records
  from {{ ref('child_table_soft_delete_monthly') }}
),

record_count_validation as (
  select 
    s.source_records,
    t.target_records,
    case 
      when s.source_records = t.target_records then 'PASS'
      else 'FAIL'
    end as record_count_status
  from source_count s
  cross join target_count t
),

data_integrity_checks as (
  select
    count(case when current_ind not in ('0', '1') then 1 end) as invalid_current_ind_count,
    count(case when dw_update_date_time is null then 1 end) as null_timestamp_count,
    count(case when child_record_id is null then 1 end) as null_primary_key_count
  from {{ ref('child_table_soft_delete_monthly') }}
)

select 
  'Data Lineage Test' as test_name,
  rcv.record_count_status,
  rcv.source_records,
  rcv.target_records,
  dic.invalid_current_ind_count,
  dic.null_timestamp_count,
  dic.null_primary_key_count,
  
  case 
    when rcv.record_count_status = 'FAIL' 
         or dic.invalid_current_ind_count > 0 
         or dic.null_timestamp_count > 0 
         or dic.null_primary_key_count > 0
    then 'FAIL'
    else 'PASS'
  end as overall_test_status

from record_count_validation rcv
cross join data_integrity_checks dic

-- Test fails if overall_test_status is 'FAIL'
having overall_test_status = 'FAIL'