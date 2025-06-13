/*
  Macro to generate current timestamp in consistent format
  Ensures consistent timestamp generation across all models
*/

{% macro generate_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

/*
  Macro for safe casting with error handling
  Equivalent to BigQuery SAFE_CAST() function
*/

{% macro safe_cast(field, target_type) %}
    TRY_CAST({{ field }} AS {{ target_type }})
{% endmacro %}