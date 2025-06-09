{% macro generate_current_timestamp() %}
  {#- 
    Macro that generates current timestamp in the format 'yyyy-MM-dd HH:mm:ss'
    This replaces the CommonTimestamp.java transform from the original Dataflow pipeline
  -#}
  
  {% set timestamp_format = var('current_timestamp_format', 'yyyy-MM-dd HH:mm:ss') %}
  
  {%- if target.type == 'snowflake' -%}
    to_varchar(current_timestamp(), '{{ timestamp_format }}')
  {%- elif target.type == 'bigquery' -%}
    format_timestamp('{{ timestamp_format }}', current_timestamp())
  {%- else -%}
    current_timestamp()
  {%- endif -%}
  
{% endmacro %}