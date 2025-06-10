-- BigQuery to Snowflake function mapping macros for UKRG LCOT UID Auth Dif processing

{# Generate UUID macro - BigQuery GENERATE_UUID() to Snowflake UUID_STRING() #}
{% macro generate_uuid() %}
  UUID_STRING()
{% endmacro %}

{# Safe casting macro - BigQuery SAFE_CAST() to Snowflake TRY_CAST() #}
{% macro safe_cast(column, data_type) %}
  TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

{# Current datetime macro - BigQuery CURRENT_DATETIME() to Snowflake CURRENT_TIMESTAMP() #}
{% macro current_datetime() %}
  CURRENT_TIMESTAMP()
{% endmacro %}

{# Current date macro - BigQuery CURRENT_DATE() to Snowflake CURRENT_DATE() #}
{% macro current_date() %}
  CURRENT_DATE()
{% endmacro %}

{# Format date macro - BigQuery FORMAT_DATE() to Snowflake TO_CHAR() #}
{% macro format_date(format_string, date_expr) %}
  TO_CHAR({{ date_expr }}, '{{ format_string | replace('%Y', 'YYYY') | replace('%m', 'MM') | replace('%d', 'DD') }}')
{% endmacro %}

{# Parse date macro - BigQuery PARSE_DATE() to Snowflake TO_DATE() #}
{% macro parse_date(format_string, date_string) %}
  TO_DATE({{ date_string }}, '{{ format_string | replace('%Y', 'YYYY') | replace('%m', 'MM') | replace('%d', 'DD') }}')
{% endmacro %}

{# Date arithmetic macros - BigQuery DATE_SUB/DATE_ADD to Snowflake DATEADD #}
{% macro date_sub(date_expr, interval_value, interval_unit) %}
  DATEADD({{ interval_unit }}, -{{ interval_value }}, {{ date_expr }})
{% endmacro %}

{% macro date_add(date_expr, interval_value, interval_unit) %}
  DATEADD({{ interval_unit }}, {{ interval_value }}, {{ date_expr }})
{% endmacro %}

{# LTRIM function - standardize trimming leading characters #}
{% macro ltrim_zeros(column) %}
  LTRIM({{ column }}, '0')
{% endmacro %}

{# REGEXP_REPLACE function - BigQuery to Snowflake regex patterns #}
{% macro regexp_replace(input_string, pattern, replacement) %}
  REGEXP_REPLACE({{ input_string }}, '{{ pattern }}', '{{ replacement }}')
{% endmacro %}

{# TO_BASE64 and SHA512 functions for hash generation #}
{% macro sha512_base64(input_expr) %}
  BASE64_ENCODE(SHA2_BINARY({{ input_expr }}, 512))
{% endmacro %}

{# SPLIT function with safe offset - BigQuery SPLIT with SAFE_OFFSET to Snowflake SPLIT_PART #}
{% macro split_safe_offset(input_string, delimiter, offset) %}
  SPLIT_PART({{ input_string }}, '{{ delimiter }}', {{ offset + 1 }})
{% endmacro %}

{# RPAD function for right padding #}
{% macro rpad(input_string, length, pad_string) %}
  RPAD({{ input_string }}, {{ length }}, '{{ pad_string }}')
{% endmacro %}

{# QUALIFY clause replacement - convert to window function with WHERE #}
{% macro qualify_to_where(window_function, condition) %}
  (
    SELECT * FROM (
      SELECT *, {{ window_function }} as row_qual
      FROM ({{ caller() }})
    ) qualified_data
    WHERE {{ condition.replace('qualify', 'row_qual') }}
  )
{% endmacro %}