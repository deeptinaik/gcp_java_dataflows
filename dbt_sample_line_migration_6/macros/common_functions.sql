{% macro generate_etl_batch_id() %}
    '{{ run_started_at.strftime("%Y%m%d%H%M%S") }}'
{% endmacro %}

{% macro current_timestamp_snowflake() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

{% macro safe_cast(column, data_type, default_value = null) %}
    COALESCE(
        TRY_CAST({{ column }} AS {{ data_type }}),
        {% if default_value %}{{ default_value }}{% else %}NULL{% endif %}
    )
{% endmacro %}

{% macro format_currency(amount_column, currency_precision = 2) %}
    ROUND({{ amount_column }}::DECIMAL(18, {{ currency_precision }}), {{ currency_precision }})
{% endmacro %}