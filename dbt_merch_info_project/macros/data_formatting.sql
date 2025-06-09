{% macro safe_trim(column) %}
  COALESCE(TRIM({{ column }}), '')
{% endmacro %}

{% macro mask_last_four(column, mask_char='X') %}
  CASE 
    WHEN LENGTH(TRIM({{ column }})) > 4 THEN 
      LPAD(SUBSTR(TRIM({{ column }}), -4), LENGTH(TRIM({{ column }})), '{{ mask_char }}')
    WHEN LENGTH(TRIM({{ column }})) < 5 THEN 
      RIGHT('0000' || TRIM({{ column }}), 4)
    ELSE TRIM({{ column }})
  END
{% endmacro %}

{% macro safe_cast_to_string(column) %}
  COALESCE(CAST({{ column }} AS STRING), '')
{% endmacro %}