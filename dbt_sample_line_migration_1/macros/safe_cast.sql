{% macro safe_cast(column_name, target_type) %}
  {%- if target_type.upper() == 'INTEGER' -%}
    try_cast({{ column_name }} as integer)
  {%- elif target_type.upper() == 'NUMERIC' -%}
    try_cast({{ column_name }} as numeric)
  {%- elif target_type.upper() == 'DATE' -%}
    try_cast({{ column_name }} as date)
  {%- elif target_type.upper() == 'TIMESTAMP' -%}
    try_cast({{ column_name }} as timestamp)
  {%- elif target_type.upper() == 'FLOAT' -%}
    try_cast({{ column_name }} as float)
  {%- else -%}
    {{ column_name }}::{{ target_type }}
  {%- endif -%}
{% endmacro %}