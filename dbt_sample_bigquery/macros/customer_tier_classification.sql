{% macro customer_tier_classification(total_spent_column) %}
  case
    when {{ total_spent_column }} > {{ var('vip_threshold') }} then 'VIP'
    when {{ total_spent_column }} > {{ var('preferred_threshold') }} then 'Preferred'
    else 'Standard'
  end
{% endmacro %}