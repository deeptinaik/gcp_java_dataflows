{% macro current_datetime() %}
  {{ return(adapter.dispatch('current_datetime', 'merch_info_dbt')()) }}
{% endmacro %}

{% macro default__current_datetime() %}
  CURRENT_TIMESTAMP()
{% endmacro %}

{% macro bigquery__current_datetime() %}
  CURRENT_DATETIME()
{% endmacro %}