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

{% macro array_agg_struct_snowflake(fields, order_by=none, limit_clause=none) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            {% for field in fields %}
                '{{ field.name }}', {{ field.value }}
                {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        )
        {% if order_by %}
        ORDER BY {{ order_by }}
        {% endif %}
        {% if limit_clause %}
        LIMIT {{ limit_clause }}
        {% endif %}
    )
{% endmacro %}

{% macro unnest_array_snowflake(table_alias, array_column) %}
    {{ table_alias }}, 
    LATERAL FLATTEN(input => {{ table_alias }}.{{ array_column }})
{% endmacro %}

{% macro array_length(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

{% macro regexp_contains(text_column, pattern) %}
    REGEXP_LIKE({{ text_column }}, '{{ pattern }}')
{% endmacro %}

{% macro left_pad(column, length, pad_char) %}
    LPAD({{ column }}, {{ length }}, '{{ pad_char }}')
{% endmacro %}

{% macro right_pad(column, length, pad_char) %}
    RPAD({{ column }}, {{ length }}, '{{ pad_char }}')
{% endmacro %}