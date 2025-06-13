{% macro customer_tier_classification(total_spent_column) %}
  case
    when {{ total_spent_column }} > 10000 then 'VIP'
    when {{ total_spent_column }} > 5000 then 'Preferred'
    else 'Standard'
  end
{% endmacro %}