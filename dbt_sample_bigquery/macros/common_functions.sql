{% macro generate_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

{% macro generate_etl_batch_id() %}
    {{ var('etl_batch_id') }}
{% endmacro %}

{% macro generate_uuid() %}
    UUID_STRING()
{% endmacro %}

{% macro safe_cast(column, data_type) %}
    TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

{% macro format_date_snowflake(date_column, format_string) %}
    TO_CHAR({{ date_column }}, '{{ format_string }}')
{% endmacro %}

{% macro parse_date_snowflake(date_string, format_string) %}
    TO_DATE({{ date_string }}, '{{ format_string }}')
{% endmacro %}

{% macro array_contains(array_column, value) %}
    ARRAY_CONTAINS({{ value }}, {{ array_column }})
{% endmacro %}

{% macro array_length(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

{% macro regexp_contains(text_column, pattern) %}
    REGEXP_LIKE({{ text_column }}, '{{ pattern }}')
{% endmacro %}

{% macro left_pad(column, length, pad_char) %}
    LPAD({{ column }}, {{ length }}, '{{ pad_char }}')
{% endmacro %}

{% macro right_pad(column, length, pad_char) %}
    RPAD({{ column }}, {{ length }}, '{{ pad_char }}')
{% endmacro %}

{% macro current_date() %}
    CURRENT_DATE()
{% endmacro %}

{% macro current_datetime() %}
    CURRENT_TIMESTAMP()
{% endmacro %}