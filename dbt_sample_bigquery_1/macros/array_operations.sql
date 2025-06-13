{% macro create_order_items_array(order_id, customer_id, order_date) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', product_id,
            'quantity', quantity,
            'price', price
        )
    ) WITHIN GROUP (ORDER BY product_id)
{% endmacro %}

{% macro create_last_orders_array(order_id, order_total, order_date, limit_count) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', {{ order_id }},
            'order_total', {{ order_total }},
            'order_date', {{ order_date }}
        )
    ) WITHIN GROUP (ORDER BY {{ order_date }} DESC)
{% endmacro %}

{% macro flatten_order_items(items_array_column) %}
    TABLE(FLATTEN({{ items_array_column }}))
{% endmacro %}

{% macro extract_from_object(object_column, key_name) %}
    {{ object_column }}:{{ key_name }}
{% endmacro %}

{% macro calculate_customer_tier(total_spent) %}
    CASE
        WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}