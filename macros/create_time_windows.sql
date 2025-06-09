{% macro create_time_windows(timestamp_column, window_size_minutes=1) %}
  -- Equivalent to Beam FixedWindows.of(Duration.standardMinutes(1))
  -- Creates time windows for aggregation
  
  DATE_TRUNC('MINUTE', {{ timestamp_column }}) +
  INTERVAL '{{ window_size_minutes }} MINUTES' * 
  FLOOR(EXTRACT(MINUTE FROM {{ timestamp_column }}) / {{ window_size_minutes }})
  
{% endmacro %}