{#-
  Custom test to validate soft delete logic implementation
  This test ensures the DBT model produces the same results as the original FullRefresh.java transform
-#}

{% test validate_soft_delete_logic(model, current_ind_column, dw_update_dtm_column) %}

  {#- Test that verifies the soft delete logic is correctly applied -#}
  
  with test_cases as (
    select 
      {{ current_ind_column }},
      {{ dw_update_dtm_column }},
      processing_timestamp,
      
      -- Expected behavior based on FullRefresh.java logic
      case 
        when {{ current_ind_column }} = '0' 
             and try_to_timestamp({{ dw_update_dtm_column }}, 'yyyy-MM-dd') < current_timestamp()
        then '1'
        else {{ current_ind_column }}
      end as expected_current_ind,
      
      case 
        when {{ current_ind_column }} = '0'
        then true  -- timestamp should be updated
        else false
      end as timestamp_should_be_updated
      
    from {{ model }}
  ),
  
  validation_failures as (
    select *
    from test_cases
    where 
      -- Check if current_ind logic is correctly applied
      {{ current_ind_column }} != expected_current_ind
      or
      -- Check if timestamp update logic is correctly applied
      (timestamp_should_be_updated = true and {{ dw_update_dtm_column }} != processing_timestamp)
  )
  
  select count(*) as failures
  from validation_failures

{% endtest %}