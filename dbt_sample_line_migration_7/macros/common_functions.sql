{% macro generate_current_timestamp() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

{% macro generate_etl_batch_id() %}
    {{ var('etl_batch_id') }}
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

{# BigQuery ARRAY_AGG(STRUCT()) equivalent in Snowflake #}
{% macro array_agg_struct(select_expression, order_by_clause=none) %}
    {%- if order_by_clause -%}
        ARRAY_AGG(OBJECT_CONSTRUCT({{ select_expression }})) WITHIN GROUP (ORDER BY {{ order_by_clause }})
    {%- else -%}
        ARRAY_AGG(OBJECT_CONSTRUCT({{ select_expression }}))
    {%- endif -%}
{% endmacro %}

{# Array operations for Snowflake #}
{% macro array_size(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

{# Handle UNNEST operations conversion #}
{% macro unnest_array(array_column, alias='value') %}
    LATERAL FLATTEN(INPUT => {{ array_column }}) AS {{ alias }}
{% endmacro %}

{# Customer tier classification macro #}
{% macro classify_customer_tier(total_spent_column) %}
    CASE
        WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}