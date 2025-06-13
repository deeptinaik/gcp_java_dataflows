-- Common BigQuery to Snowflake function mappings

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

{% macro array_contains(array_column, value) %}
    ARRAY_CONTAINS({{ value }}, {{ array_column }})
{% endmacro %}

{% macro array_length(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

{% macro regexp_contains(text_column, pattern) %}
    REGEXP_LIKE({{ text_column }}, '{{ pattern }}')
{% endmacro %}

-- BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT(...))
{% macro array_agg_struct(fields_list, order_by_clause=none, limit_clause=none) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            {%- for field in fields_list %}
            '{{ field.name }}', {{ field.value }}
            {%- if not loop.last -%},{%- endif -%}
            {%- endfor %}
        )
        {%- if order_by_clause %} ORDER BY {{ order_by_clause }}{% endif %}
        {%- if limit_clause %} LIMIT {{ limit_clause }}{% endif %}
    )
{% endmacro %}

-- Convert BigQuery UNNEST to Snowflake LATERAL FLATTEN
{% macro lateral_flatten_array(table_alias, array_column, value_alias='value') %}
    LATERAL FLATTEN(INPUT => {{ table_alias }}.{{ array_column }}) AS {{ value_alias }}
{% endmacro %}