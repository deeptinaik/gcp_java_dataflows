-- Common utility functions for BigQuery to Snowflake conversion

{% macro generate_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

{% macro generate_etl_batch_id() %}
    {{ var('etl_batch_id') }}
{% endmacro %}

{% macro generate_uuid() %}
    UUID_STRING()
{% endmacro %}

{% macro safe_cast(column, data_type) %}
    TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

{% macro format_date_snowflake(date_column, format_string) %}
    TO_CHAR({{ date_column }}, '{{ format_string }}')
{% endmacro %}

{% macro parse_date_snowflake(date_string, format_string) %}
    TO_DATE({{ date_string }}, '{{ format_string }}')
{% endmacro %}

-- BigQuery ARRAY_AGG to Snowflake conversion with object structure
{% macro array_agg_struct(columns, order_by=None, limit=None) %}
  {% if limit %}
    ARRAY_SLICE(
      ARRAY_AGG(
        OBJECT_CONSTRUCT(
          {% for col in columns %}
            '{{ col.name }}', {{ col.value }}
            {%- if not loop.last -%}, {% endif %}
          {% endfor %}
        )
        {% if order_by %}
          ORDER BY {{ order_by }}
        {% endif %}
      ),
      0, {{ limit }}
    )
  {% else %}
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        {% for col in columns %}
          '{{ col.name }}', {{ col.value }}
          {%- if not loop.last -%}, {% endif %}
        {% endfor %}
      )
      {% if order_by %}
        ORDER BY {{ order_by }}
      {% endif %}
    )
  {% endif %}
{% endmacro %}

-- Convert BigQuery UNNEST to Snowflake FLATTEN
{% macro unnest_to_flatten(array_column, path=None) %}
  FLATTEN({{ array_column }}
    {% if path %}
      , '{{ path }}'
    {% endif %}
  )
{% endmacro %}

-- Customer tier classification macro
{% macro classify_customer_tier(total_spent_column) %}
  CASE
    WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
    WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
    ELSE '{{ var('default_tier') }}'
  END
{% endmacro %}

-- Array contains functionality for Snowflake
{% macro array_contains(array_column, value) %}
    ARRAY_CONTAINS({{ value }}, {{ array_column }})
{% endmacro %}

-- Array size functionality for Snowflake
{% macro array_size(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

-- Regexp functionality conversion
{% macro regexp_contains(text_column, pattern) %}
    REGEXP_LIKE({{ text_column }}, '{{ pattern }}')
{% endmacro %}