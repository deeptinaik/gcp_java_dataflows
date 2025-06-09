{% macro generate_uuid() %}
  {{ return(adapter.dispatch('generate_uuid', 'merch_info_dbt')()) }}
{% endmacro %}

{% macro default__generate_uuid() %}
  UUID_STRING()
{% endmacro %}

{% macro bigquery__generate_uuid() %}
  GENERATE_UUID()
{% endmacro %}