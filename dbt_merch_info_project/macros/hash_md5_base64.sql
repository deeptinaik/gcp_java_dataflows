{% macro hash_md5_base64(column_list) %}
  {{ return(adapter.dispatch('hash_md5_base64', 'merch_info_dbt')(column_list)) }}
{% endmacro %}

{% macro default__hash_md5_base64(column_list) %}
  BASE64_ENCODE(MD5(CONCAT({{ column_list }})))
{% endmacro %}

{% macro bigquery__hash_md5_base64(column_list) %}
  TO_BASE64(MD5(CONCAT({{ column_list }})))
{% endmacro %}