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

-- BigQuery ARRAY_AGG with STRUCT to Snowflake conversion
{% macro array_agg_struct(columns, order_by_clause=none, limit_clause=none) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            {% for column in columns %}
            '{{ column.name }}', {{ column.expr }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        )
        {% if order_by_clause %}
        ORDER BY {{ order_by_clause }}
        {% endif %}
    )
    {% if limit_clause %}
    [0:{{ limit_clause - 1 }}]
    {% endif %}
{% endmacro %}

-- Convert BigQuery UNNEST to Snowflake LATERAL FLATTEN
{% macro unnest_array(table_alias, array_column) %}
    LATERAL FLATTEN(input => {{ table_alias }}.{{ array_column }})
{% endmacro %}

-- Array length function
{% macro array_length(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}

-- Rank window function (same in both platforms)
{% macro rank_over(partition_by, order_by) %}
    RANK() OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }})
{% endmacro %}

-- Generate customer tier classification
{% macro customer_tier_classification(amount_column) %}
    CASE
        WHEN {{ amount_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ amount_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}