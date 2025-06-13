{% macro create_order_items_array(product_id, quantity, price) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', {{ product_id }},
            'quantity', {{ quantity }},
            'price', {{ price }}
        )
    )
{% endmacro %}

{% macro create_order_summary_array(order_id, order_total, order_date, limit_count) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', {{ order_id }},
            'order_total', {{ order_total }},
            'order_date', {{ order_date }}
        )
    ) WITHIN GROUP (ORDER BY {{ order_date }} DESC)
    {% if limit_count %}
    [0:{{ limit_count - 1 }}]
    {% endif %}
{% endmacro %}

{% macro calculate_customer_tier(total_spent) %}
    CASE
        WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}

{% macro calculate_line_total(quantity, price) %}
    ({{ quantity }} * {{ price }})
{% endmacro %}