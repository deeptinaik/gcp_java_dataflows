-- Common utility functions for data transformation

-- Safe casting function (replacing BigQuery SAFE_CAST)
{% macro safe_cast(column, data_type) %}
  TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

-- Generate current timestamp
{% macro generate_current_timestamp() %}
  CURRENT_TIMESTAMP()
{% endmacro %}

-- Calculate line total (quantity * price)
{% macro calculate_line_total(quantity_column, price_column) %}
  ({{ quantity_column }} * {{ price_column }})
{% endmacro %}