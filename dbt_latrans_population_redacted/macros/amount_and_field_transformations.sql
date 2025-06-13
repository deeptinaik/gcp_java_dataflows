{#
  Amount transformation macro that converts raw integer amounts to decimal with scale
  Replicates Java BigDecimal.scaleByPowerOfTen(-exponent) logic
  Original Java logic from FieldTransformation.java lines 70-78, 80-86
#}
{% macro transform_amount_with_scale(amount_field, exponent_value) %}
  case 
    when {{ amount_field }} is not null
    then cast(cast({{ amount_field }} as numeric) / pow(10, {{ exponent_value }}) as string)
    else null
  end
{% endmacro %}

{#
  Authorization amount transformation that handles decimal point insertion
  Original Java logic from FieldTransformation.java lines 88-101
#}
{% macro transform_authorization_amount(amount_field) %}
  case 
    when {{ amount_field }} is not null 
      and trim(cast({{ amount_field }} as string)) != ''
    then concat(
      substr(trim(cast({{ amount_field }} as string)), 1, 7),
      '.',
      substr(trim(cast({{ amount_field }} as string)), 8)
    )
    else '0'
  end
{% endmacro %}

{#
  Batch control number creation macro
  Replicates the createBatchCntrlNum() method from FieldTransformation.java lines 205-222
#}
{% macro create_batch_control_number(deposit_date_field, cash_letter_number_field) %}
  case 
    when {{ deposit_date_field }} is not null and {{ cash_letter_number_field }} is not null
    then concat(
      substr(trim(cast({{ deposit_date_field }} as string)), 1, 2),  -- First 2 chars of deposit date
      cast(extract(dayofyear from safe.parse_date('%Y%m%d', concat('20', trim(cast({{ deposit_date_field }} as string))))) as string),  -- Day of year
      substr(trim(cast({{ cash_letter_number_field }} as string)), 4, 6)  -- Characters 3-8 of cash letter number
    )
    else null
  end
{% endmacro %}

{#
  Time validation and formatting macro
  Replicates Utility.getValidTime() functionality
#}
{% macro get_valid_time(time_field) %}
  case 
    when {{ time_field }} is not null 
      and trim(cast({{ time_field }} as string)) != ''
      and safe.parse_time('%H%M%S', lpad(trim(cast({{ time_field }} as string)), 6, '0')) is not null
    then format_time('%H:%M:%S', safe.parse_time('%H%M%S', lpad(trim(cast({{ time_field }} as string)), 6, '0')))
    else null
  end
{% endmacro %}

{#
  Transaction identifier cleaning macro
  Removes spaces from transaction identifier
  Original Java logic from FieldTransformation.java lines 168-174
#}
{% macro clean_transaction_identifier(transaction_id_field) %}
  case 
    when {{ transaction_id_field }} is not null
    then replace(trim(cast({{ transaction_id_field }} as string)), ' ', '')
    else ''
  end
{% endmacro %}

{#
  Merchant number integer conversion with leading zero trimming
  Replicates Utility.trimLeadingZeros() functionality
  Original Java logic from FieldTransformation.java lines 176-179
#}
{% macro trim_leading_zeros_merchant_number(merchant_number_field) %}
  case 
    when {{ merchant_number_field }} is not null
    then cast(cast(trim(cast({{ merchant_number_field }} as string)) as int64) as string)
    else null
  end
{% endmacro %}

{#
  Corporate region concatenation for default currency lookup
  Used in dimension joins for currency code resolution
#}
{% macro create_corporate_region_key(corporate_field, region_field) %}
  concat(
    cast({{ corporate_field }} as string),
    cast({{ region_field }} as string)
  )
{% endmacro %}