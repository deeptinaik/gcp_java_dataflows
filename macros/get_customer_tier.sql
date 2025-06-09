{% macro get_customer_tier(total_spent_field) %}
    CASE
        WHEN {{ total_spent_field }} > 10000 THEN 'VIP'
        WHEN {{ total_spent_field }} > 5000 THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}