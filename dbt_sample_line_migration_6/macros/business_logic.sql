{% macro customer_tier_classification(total_spent_column) %}
    CASE
        WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}

{% macro aggregate_order_details(order_id_col, order_total_col, order_date_col, limit_value) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'order_id', {{ order_id_col }},
            'order_total', {{ order_total_col }},
            'order_date', {{ order_date_col }}
        )
    ) WITHIN GROUP (ORDER BY {{ order_date_col }} DESC)
{% endmacro %}

{% macro safe_divide(numerator, denominator, default_value = 0) %}
    CASE 
        WHEN {{ denominator }} = 0 THEN {{ default_value }}
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}