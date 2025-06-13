-- Business logic macros for sales analytics

{% macro create_order_items_array(product_id, quantity, price, order_by=None) %}
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'product_id', {{ product_id }},
      'quantity', {{ quantity }},
      'price', {{ price }}
    )
    {% if order_by %}
      ORDER BY {{ order_by }}
    {% endif %}
  )
{% endmacro %}

{% macro create_recent_orders_array(order_id, order_total, order_date, limit=3) %}
  ARRAY_SLICE(
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'order_id', {{ order_id }},
        'order_total', {{ order_total }},
        'order_date', {{ order_date }}
      )
      ORDER BY {{ order_date }} DESC
    ),
    0, {{ limit }}
  )
{% endmacro %}

{% macro calculate_order_total(quantity, price) %}
  SUM({{ quantity }} * {{ price }})
{% endmacro %}

{% macro rank_orders_by_date(partition_by, order_by) %}
  RANK() OVER (
    PARTITION BY {{ partition_by }} 
    ORDER BY {{ order_by }} DESC
  )
{% endmacro %}

-- Validate positive amounts
{% macro validate_positive_amount(amount_column) %}
  {{ amount_column }} > 0
{% endmacro %}

-- Check for valid customer tier values
{% macro valid_customer_tiers() %}
  ('VIP', 'Preferred', 'Standard')
{% endmacro %}

-- Business validation for order data
{% macro validate_order_data() %}
  order_id IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND order_date IS NOT NULL
{% endmacro %}