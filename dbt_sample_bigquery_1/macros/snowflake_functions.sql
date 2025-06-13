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

{% macro array_agg_struct_to_object(field_list, order_by_clause=none, limit_clause=none) %}
    {%- set fields_array = field_list.split(',') -%}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            {%- for field in fields_array -%}
                {%- set field_clean = field.strip() -%}
                {%- if field_clean.find(' AS ') != -1 -%}
                    {%- set field_parts = field_clean.split(' AS ') -%}
                    '{{ field_parts[1].strip() }}', {{ field_parts[0].strip() }}
                {%- else -%}
                    '{{ field_clean }}', {{ field_clean }}
                {%- endif -%}
                {%- if not loop.last -%}, {% endif -%}
            {%- endfor -%}
        )
        {%- if order_by_clause %} {{ order_by_clause }}{% endif -%}
    )
    {%- if limit_clause %} {{ limit_clause }}{% endif -%}
{% endmacro %}

{% macro unnest_array_to_lateral(array_column, alias_name='value') %}
    , LATERAL FLATTEN(input => {{ array_column }}) AS {{ alias_name }}
{% endmacro %}

{% macro extract_from_object(object_column, field_name) %}
    {{ object_column }}:{{ field_name }}
{% endmacro %}