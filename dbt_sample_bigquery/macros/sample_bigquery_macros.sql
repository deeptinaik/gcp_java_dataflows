-- Customer tier classification macro
-- Replicates the customer tier logic from the original BigQuery

{% macro classify_customer_tier(total_spent_column) %}
    
    CASE
        WHEN {{ total_spent_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END

{% endmacro %}

-- Generate UUID function for Snowflake
-- Converts BigQuery GENERATE_UUID() to Snowflake UUID_STRING()

{% macro generate_uuid() %}
    UUID_STRING()
{% endmacro %}

-- Safe cast function for Snowflake
-- Converts BigQuery SAFE_CAST() to Snowflake TRY_CAST()

{% macro safe_cast(column, data_type) %}
    TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

-- Current datetime function for Snowflake
-- Converts BigQuery CURRENT_DATETIME() to Snowflake CURRENT_TIMESTAMP()

{% macro current_datetime() %}
    CURRENT_TIMESTAMP()
{% endmacro %}

-- Array size function for Snowflake
-- Converts BigQuery ARRAY_LENGTH() to Snowflake ARRAY_SIZE()

{% macro array_length(array_column) %}
    ARRAY_SIZE({{ array_column }})
{% endmacro %}