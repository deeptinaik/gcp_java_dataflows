{% macro generate_etl_batch_date() %}
  parse_date('%Y%m%d', left({{ generate_etl_batch_id() }}, 8))
{% endmacro %}