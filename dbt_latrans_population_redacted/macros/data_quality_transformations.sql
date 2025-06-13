{#
  Hierarchy and data quality macros
  Additional helper functions for complex transformations
#}

{#
  Hierarchy field concatenation macro
  Replicates the Java StringBuilder logic for hierarchy field creation
  Original Java logic from FieldTransformation.java lines 182-188
#}
{% macro build_hierarchy_field(corp_field, region_field, principal_field, associate_field, chain_field) %}
  concat(
    coalesce(cast({{ corp_field }} as string), ''), ',',
    coalesce(cast({{ region_field }} as string), ''), ',',
    coalesce(cast({{ principal_field }} as string), ''), ',',
    coalesce(cast({{ associate_field }} as string), ''), ',',
    coalesce(cast({{ chain_field }} as string), '')
  )
{% endmacro %}

{#
  Data quality check macro for numeric fields
  Ensures numeric fields are valid and within acceptable ranges
#}
{% macro validate_numeric_field(field_name, min_value=null, max_value=null) %}
  case 
    when {{ field_name }} is null then null
    when safe.cast({{ field_name }} as numeric) is null then null
    {% if min_value is not none %}
    when safe.cast({{ field_name }} as numeric) < {{ min_value }} then null
    {% endif %}
    {% if max_value is not none %}
    when safe.cast({{ field_name }} as numeric) > {{ max_value }} then null
    {% endif %}
    else safe.cast({{ field_name }} as numeric)
  end
{% endmacro %}

{#
  Null handling macro for string fields
  Standardizes null and blank handling across the pipeline
#}
{% macro handle_null_string(field_name, default_value='') %}
  case 
    when {{ field_name }} is null then '{{ default_value }}'
    when trim(cast({{ field_name }} as string)) = '' then '{{ default_value }}'
    else trim(cast({{ field_name }} as string))
  end
{% endmacro %}

{#
  Lookup failure handling macro
  Standardizes how failed dimension lookups are handled
#}
{% macro handle_lookup_failure(lookup_result, failure_value) %}
  coalesce({{ lookup_result }}, '{{ failure_value }}')
{% endmacro %}

{#
  Generate hash key macro (replacement for Java hash key logic)
  Creates a consistent hash-based unique identifier
#}
{% macro generate_transaction_hash(field_list) %}
  farm_fingerprint(
    concat(
      {% for field in field_list %}
      coalesce(cast({{ field }} as string), '')
      {%- if not loop.last -%},{%- endif -%}
      {% endfor %}
    )
  )
{% endmacro %}

{#
  Date range validation macro
  Ensures dates fall within acceptable business ranges
#}
{% macro validate_date_range(date_field, min_year=1900, max_year=2099) %}
  case 
    when {{ date_field }} is null then null
    when safe.parse_date('%Y%m%d', {{ date_field }}) is null then null
    when extract(year from safe.parse_date('%Y%m%d', {{ date_field }})) < {{ min_year }} then null
    when extract(year from safe.parse_date('%Y%m%d', {{ date_field }})) > {{ max_year }} then null
    else safe.parse_date('%Y%m%d', {{ date_field }})
  end
{% endmacro %}