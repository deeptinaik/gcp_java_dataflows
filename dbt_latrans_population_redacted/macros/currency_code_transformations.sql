{#
  Currency code validation macros
  Replicates the currency code validation logic from the Java pipeline
#}

{#
  Check if currency code is numeric
  Used in FilterCurrencyCodeByAlphaAndNumeric transformation
#}
{% macro is_numeric_currency_code(currency_code_field) %}
  safe.cast({{ currency_code_field }} as int64) is not null
{% endmacro %}

{#
  Check if currency code is alphabetic
  Used in FilterCurrencyCodeByAlphaAndNumeric transformation
#}
{% macro is_alpha_currency_code(currency_code_field) %}
  {{ currency_code_field }} is not null 
  and trim(cast({{ currency_code_field }} as string)) != ''
  and safe.cast({{ currency_code_field }} as int64) is null
  and regexp_contains(trim(cast({{ currency_code_field }} as string)), r'^[A-Za-z]+$')
{% endmacro %}

{#
  Check if currency code is null or blank
  Used in FilterCurrencyCodeByAlphaAndNumeric transformation
#}
{% macro is_null_blank_currency_code(currency_code_field) %}
  {{ currency_code_field }} is null 
  or trim(cast({{ currency_code_field }} as string)) = ''
{% endmacro %}

{#
  Currency code classification macro
  Returns 'alpha', 'numeric', or 'null' based on currency code type
#}
{% macro classify_currency_code(currency_code_field) %}
  case 
    when {{ is_null_blank_currency_code(currency_code_field) }}
    then 'null'
    when {{ is_numeric_currency_code(currency_code_field) }}
    then 'numeric'
    when {{ is_alpha_currency_code(currency_code_field) }}
    then 'alpha'
    else 'null'
  end
{% endmacro %}

{#
  Settlement currency code classification macro
  Same logic as currency code but for settlement currency
#}
{% macro classify_settlement_currency_code(settle_currency_code_field) %}
  case 
    when {{ is_null_blank_currency_code(settle_currency_code_field) }}
    then 'null'
    when {{ is_numeric_currency_code(settle_currency_code_field) }}
    then 'numeric'
    when {{ is_alpha_currency_code(settle_currency_code_field) }}
    then 'alpha'
    else 'null'
  end
{% endmacro %}

{#
  Updated currency code field assignment
  Sets the appropriate currency code based on validation results
#}
{% macro set_updated_currency_code(original_currency_code, default_currency_code) %}
  case 
    when {{ is_null_blank_currency_code(original_currency_code) }}
      and {{ default_currency_code }} is not null
    then {{ default_currency_code }}
    else {{ original_currency_code }}
  end
{% endmacro %}