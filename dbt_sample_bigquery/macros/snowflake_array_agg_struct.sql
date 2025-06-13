{% macro snowflake_array_agg_struct(order_id, total, order_date) %}
  array_agg(
    object_construct(
      'order_id', {{ order_id }},
      'order_total', {{ total }},
      'order_date', {{ order_date }}
    )
  )
{% endmacro %}