{% macro aggregate_recent_orders(orders_json_column, order_date_column, order_total_column, limit_value) %}
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'order_id', order_id,
      'order_total', {{ order_total_column }},
      'order_date', {{ order_date_column }}
    )
  ) WITHIN GROUP (ORDER BY {{ order_date_column }} DESC)
{% endmacro %}