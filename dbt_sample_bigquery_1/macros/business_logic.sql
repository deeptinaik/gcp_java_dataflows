{% macro calculate_customer_tier(total_spent_column) %}
    CASE
        WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}

{% macro calculate_order_total(quantity_column, price_column) %}
    {{ quantity_column }} * {{ price_column }}
{% endmacro %}

{% macro rank_orders_by_date(customer_id_column, order_date_column) %}
    RANK() OVER (
        PARTITION BY {{ customer_id_column }} 
        ORDER BY {{ order_date_column }} DESC
    )
{% endmacro %}

{% macro filter_recent_orders(rank_column, max_orders=3) %}
    {{ rank_column }} <= {{ max_orders }}
{% endmacro %}