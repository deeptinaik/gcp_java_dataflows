{% macro parse_date_from_string(format_string, date_string) %}
  {{ return(adapter.dispatch('parse_date_from_string', 'merch_info_dbt')(format_string, date_string)) }}
{% endmacro %}

{% macro default__parse_date_from_string(format_string, date_string) %}
  TO_DATE({{ date_string }}, '{{ format_string }}')
{% endmacro %}

{% macro bigquery__parse_date_from_string(format_string, date_string) %}
  PARSE_DATE('{{ format_string }}', {{ date_string }})
{% endmacro %}