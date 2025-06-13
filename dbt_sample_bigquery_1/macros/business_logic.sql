{% macro calculate_customer_tier(total_spent) %}
    CASE
        WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}

{% macro get_last_n_orders(order_id, order_total, order_date, limit_count=3) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', {{ order_id }},
            'order_total', {{ order_total }},
            'order_date', {{ order_date }}
        )
        ORDER BY {{ order_date }} DESC
    ) WITHIN GROUP (ORDER BY {{ order_date }} DESC)
{% endmacro %}

{% macro format_currency_amount(amount_column) %}
    ROUND({{ amount_column }}, 2)
{% endmacro %}