-- Business logic macros for customer analysis

{% macro classify_customer_tier(total_spent_column) %}
    CASE
        WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}

{% macro calculate_order_total(quantity_col, price_col) %}
    ({{ quantity_col }} * {{ price_col }})
{% endmacro %}

-- Macro to validate customer tier values
{% macro get_valid_customer_tiers() %}
    ('VIP', 'Preferred', 'Standard')
{% endmacro %}