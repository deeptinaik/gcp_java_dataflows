{% macro customer_tier_classification(total_spent) %}
  CASE
    WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
    WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
    ELSE 'Standard'
  END
{% endmacro %}

{% macro aggregate_order_items(product_id, quantity, price) %}
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'product_id', {{ product_id }},
      'quantity', {{ quantity }},
      'price', {{ price }}
    )
  )
{% endmacro %}

{% macro calculate_order_total(items_array) %}
  SUM(
    (items.value:quantity)::NUMBER * (items.value:price)::NUMBER
  )
{% endmacro %}

{% macro get_last_n_orders(order_id, order_total, order_date, n) %}
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'order_id', {{ order_id }},
      'order_total', {{ order_total }},
      'order_date', {{ order_date }}
    )
    ORDER BY {{ order_date }} DESC 
    LIMIT {{ n }}
  )
{% endmacro %}

{% macro rank_orders_by_date(customer_id, order_date) %}
  RANK() OVER (
    PARTITION BY {{ customer_id }} 
    ORDER BY {{ order_date }} DESC
  )
{% endmacro %}