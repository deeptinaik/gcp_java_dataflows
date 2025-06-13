{% macro array_agg_struct_snowflake(order_id, order_total, order_date, order_by_clause=null, limit_clause=null) %}
  {%- set order_clause = order_by_clause if order_by_clause else order_date + " DESC" -%}
  {%- set limit_clause = limit_clause if limit_clause else "3" -%}
  
  array_agg(
    object_construct(
      'order_id', {{ order_id }},
      'order_total', {{ order_total }},
      'order_date', {{ order_date }}
    )
  ) within group (order by {{ order_clause }})
{% endmacro %}