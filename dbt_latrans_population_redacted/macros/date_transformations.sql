{#
  Date transformation macro that replicates the Java logic for transaction_date and file_date_original
  Converts MMDDYY format to YYYYMMDD format 
  Original Java logic from FieldTransformation.java lines 58-67
#}
{% macro transform_date_mmddyy_to_yyyymmdd(input_date_field) %}
  case 
    when {{ input_date_field }} is not null 
      and trim(cast({{ input_date_field }} as string)) != ''
      and length(trim(cast({{ input_date_field }} as string))) = 6
    then concat(
      '20',
      substr(trim(cast({{ input_date_field }} as string)), 5, 2),  -- year (YY)
      substr(trim(cast({{ input_date_field }} as string)), 1, 2),  -- month (MM)
      substr(trim(cast({{ input_date_field }} as string)), 3, 2)   -- day (DD)
    )
    else ''
  end
{% endmacro %}

{#
  Date transformation macro for deposit_date and file_date
  Converts YYMMDD format to YYYYMMDD format
  Original Java logic from FieldTransformation.java lines 117-128
#}
{% macro transform_date_yymmdd_to_yyyymmdd(input_date_field) %}
  case 
    when {{ input_date_field }} is not null 
      and trim(cast({{ input_date_field }} as string)) != ''
      and length(trim(cast({{ input_date_field }} as string))) = 6
    then concat('20', trim(cast({{ input_date_field }} as string)))
    else ''
  end
{% endmacro %}

{#
  Lodging and car rental date transformation
  Converts YYMMDD format to YYYYMMDD format for lodging_checkin_date and car_rental_checkout_date
  Original Java logic from FieldTransformation.java lines 103-115
#}
{% macro transform_lodging_car_rental_date(input_date_field) %}
  case 
    when {{ input_date_field }} is not null 
      and trim(cast({{ input_date_field }} as string)) != ''
      and length(trim(cast({{ input_date_field }} as string))) = 6
    then concat('20', trim(cast({{ input_date_field }} as string)))
    else ''
  end
{% endmacro %}

{#
  Authorization date transformation with complex year logic
  Original Java logic from FieldTransformation.java lines 130-151
#}
{% macro transform_authorization_date(transaction_date_field, auth_date_field) %}
  case 
    when {{ transaction_date_field }} is not null 
      and trim(cast({{ transaction_date_field }} as string)) != ''
      and {{ auth_date_field }} is not null 
      and trim(cast({{ auth_date_field }} as string)) != ''
    then 
      case 
        -- Handle year boundary case: Jan transaction with Dec auth date means previous year
        when substr(trim(cast({{ transaction_date_field }} as string)), 1, 2) = '01'
          and substr(trim(cast({{ auth_date_field }} as string)), 1, 2) = '12'
        then concat(
          '20',
          cast(cast(substr(trim(cast({{ transaction_date_field }} as string)), 5, 2) as int64) - 1 as string),
          trim(cast({{ auth_date_field }} as string))
        )
        else concat(
          '20',
          substr(trim(cast({{ transaction_date_field }} as string)), 5, 2),
          trim(cast({{ auth_date_field }} as string))
        )
      end
    else null
  end
{% endmacro %}

{#
  Valid date check macro (basic validation)
  Replicates Utility.isValidDate() functionality
#}
{% macro is_valid_date(date_string) %}
  safe.parse_date('%Y%m%d', {{ date_string }}) is not null
{% endmacro %}