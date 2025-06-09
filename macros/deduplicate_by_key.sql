{% macro deduplicate_by_key(table_name, key_column, timestamp_column, hours_window=10) %}
  -- Equivalent to DeduplicateFn.java stateful deduplication
  -- Uses ROW_NUMBER() window function to keep latest record within time window
  
  WITH ranked_records AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY {{ key_column }}
        ORDER BY {{ timestamp_column }} DESC
      ) AS row_num
    FROM {{ table_name }}
    WHERE {{ timestamp_column }} >= CURRENT_TIMESTAMP - INTERVAL '{{ hours_window }} HOURS'
  )
  SELECT *
  FROM ranked_records  
  WHERE row_num = 1
{% endmacro %}