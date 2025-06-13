-- Macro to classify customer tiers based on total spending
{% macro customer_tier_classification(total_spent_column) %}
  CASE
    WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
    WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
    ELSE 'Standard'
  END
{% endmacro %}

-- Macro to create order structure for aggregation (replacing BigQuery STRUCT)
{% macro create_order_struct(order_id, order_total, order_date) %}
  OBJECT_CONSTRUCT(
    'order_id', {{ order_id }},
    'order_total', {{ order_total }},
    'order_date', {{ order_date }}
  )
{% endmacro %}

-- Macro to aggregate orders into array (replacing BigQuery ARRAY_AGG with STRUCT)
{% macro aggregate_recent_orders(order_struct, order_date_column, limit_count) %}
  ARRAY_AGG({{ order_struct }}) 
  WITHIN GROUP (ORDER BY {{ order_date_column }} DESC)
{% endmacro %}