{% macro validate_transaction_data(amount_column, location_column, customer_id_column) %}
  -- Equivalent to AmountValidator, LocationValidator, etc. from complex fraud detection
  -- Returns validation status for transaction data
  
  CASE
    WHEN {{ amount_column }} IS NULL THEN 'INVALID_NULL_AMOUNT'
    WHEN {{ amount_column }} <= 0 THEN 'INVALID_NEGATIVE_AMOUNT'
    WHEN {{ amount_column }} > 1000000 THEN 'INVALID_EXCESSIVE_AMOUNT'
    
    WHEN {{ location_column }} IS NULL THEN 'INVALID_NULL_LOCATION'
    WHEN LENGTH({{ location_column }}) < 2 THEN 'INVALID_SHORT_LOCATION'
    
    WHEN {{ customer_id_column }} IS NULL THEN 'INVALID_NULL_CUSTOMER'
    WHEN LENGTH({{ customer_id_column }}) < 3 THEN 'INVALID_SHORT_CUSTOMER_ID'
    
    ELSE 'VALID'
  END
{% endmacro %}