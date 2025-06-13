{% macro customer_tier_classification(total_spent) %}
  CASE
    WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
    WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
    ELSE 'Standard'
  END
{% endmacro %}