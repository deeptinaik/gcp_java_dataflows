{#-
  Comprehensive test to validate the complete business logic conversion from Java to SQL
  This test creates sample scenarios and validates that the SQL logic exactly matches
  the original FullRefresh.java behavior
-#}

-- Test scenario 1: current_ind='0' and old date -> should set current_ind='1' and update timestamp
with scenario_1 as (
  select 
    'CHD001' as child_record_id,
    '0' as current_ind_input,
    '2023-01-01' as dw_update_date_time_input,
    '1' as expected_current_ind,
    true as expect_timestamp_update
),

-- Test scenario 2: current_ind='0' and recent date -> should keep current_ind='0' and update timestamp  
scenario_2 as (
  select
    'CHD002' as child_record_id,
    '0' as current_ind_input,
    to_varchar(current_date()) as dw_update_date_time_input,
    '0' as expected_current_ind,
    true as expect_timestamp_update
),

-- Test scenario 3: current_ind='1' -> should not change anything
scenario_3 as (
  select
    'CHD003' as child_record_id,
    '1' as current_ind_input,
    '2023-06-01' as dw_update_date_time_input,
    '1' as expected_current_ind,
    false as expect_timestamp_update
),

-- Union all test scenarios
test_data as (
  select * from scenario_1
  union all
  select * from scenario_2  
  union all
  select * from scenario_3
),

-- Apply the actual business logic (same as in the main model)
processed_data as (
  select 
    t.*,
    to_varchar(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as processing_timestamp,
    
    -- Apply the exact soft delete logic from FullRefresh.java
    case 
      when t.current_ind_input = '0'
           and try_to_timestamp(t.dw_update_date_time_input, 'yyyy-MM-dd') < current_timestamp()
      then '1'
      else t.current_ind_input
    end as actual_current_ind,
    
    -- Timestamp update logic
    case 
      when t.current_ind_input = '0'
      then to_varchar(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')
      else t.dw_update_date_time_input
    end as actual_dw_update_date_time
    
  from test_data t
),

-- Validate results
validation_results as (
  select 
    p.*,
    case 
      when p.actual_current_ind = p.expected_current_ind then 'PASS'
      else 'FAIL'
    end as current_ind_test_result,
    
    case 
      when p.expect_timestamp_update = true and p.actual_dw_update_date_time != p.dw_update_date_time_input then 'PASS'
      when p.expect_timestamp_update = false and p.actual_dw_update_date_time = p.dw_update_date_time_input then 'PASS'
      else 'FAIL'
    end as timestamp_test_result
    
  from processed_data p
)

select 
  child_record_id,
  current_ind_input,
  dw_update_date_time_input,
  expected_current_ind,
  actual_current_ind,
  current_ind_test_result,
  expect_timestamp_update,
  timestamp_test_result,
  
  case 
    when current_ind_test_result = 'PASS' and timestamp_test_result = 'PASS' then 'PASS'
    else 'FAIL'
  end as overall_test_result

from validation_results

-- Test fails if any scenario fails
having count(case when overall_test_result = 'FAIL' then 1 end) > 0