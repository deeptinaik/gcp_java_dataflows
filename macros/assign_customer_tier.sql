{% macro assign_customer_tier(amount_column) %}
  -- Equivalent to AddCustomerTierFn.java logic
  -- if (event.amount > 500) tier = "GOLD";
  -- else if (event.amount > 100) tier = "SILVER"; 
  -- else tier = "BRONZE";
  
  CASE
    WHEN {{ amount_column }} > {{ var('customer_tier_gold_threshold') }} THEN 'GOLD'
    WHEN {{ amount_column }} > {{ var('customer_tier_silver_threshold') }} THEN 'SILVER'
    ELSE 'BRONZE'
  END
{% endmacro %}