{% macro apply_soft_delete_logic(table_name, current_ind_field='current_ind', dw_update_dtm_field='dw_update_date_time') %}
  {#-
    Macro that applies soft delete logic similar to FullRefresh.java transform
    
    Business Logic:
    1. If record date is before current time AND current_ind="0", set current_ind="1" and update timestamp
    2. If current_ind="0" (regardless of date), update the timestamp
    3. Return the modified record
    
    Parameters:
    - table_name: Source table name to read from
    - current_ind_field: Field name for current indicator (default: 'current_ind')
    - dw_update_dtm_field: Field name for update datetime (default: 'dw_update_date_time')
  -#}

  select 
    *,
    -- Apply soft delete logic based on conditions
    case 
      when {{ current_ind_field }} = '{{ var("current_ind_inactive", "0") }}'
           and try_to_timestamp({{ dw_update_dtm_field }}, '{{ var("date_format", "yyyy-MM-dd") }}') < current_timestamp()
      then '{{ var("current_ind_active", "1") }}'
      else {{ current_ind_field }}
    end as {{ current_ind_field }}_updated,
    
    -- Update timestamp when current_ind is 0
    case 
      when {{ current_ind_field }} = '{{ var("current_ind_inactive", "0") }}'
      then {{ generate_current_timestamp() }}
      else {{ dw_update_dtm_field }}
    end as {{ dw_update_dtm_field }}_updated,
    
    -- Generate current processing timestamp for audit
    {{ generate_current_timestamp() }} as processing_timestamp
    
  from {{ table_name }}

{% endmacro %}