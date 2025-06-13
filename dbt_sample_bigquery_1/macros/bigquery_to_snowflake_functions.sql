/*
  Macro to convert BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake equivalent
  BigQuery: ARRAY_AGG(STRUCT(product_id, quantity, price))
  Snowflake: ARRAY_AGG(OBJECT_CONSTRUCT('product_id', product_id, 'quantity', quantity, 'price', price))
*/

{% macro array_agg_struct_snowflake(field1, field2, field3) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            '{{ field1 }}', {{ field1 }},
            '{{ field2 }}', {{ field2 }},
            '{{ field3 }}', {{ field3 }}
        )
    )
{% endmacro %}

/*
  Macro for aggregating objects with specific field names for recent orders
*/
{% macro snowflake_array_agg_object(id_field, total_field, date_field) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            '{{ id_field }}', {{ id_field }},
            '{{ total_field }}', {{ total_field }},
            '{{ date_field }}', {{ date_field }}
        ) 
        ORDER BY {{ date_field }} DESC
        LIMIT {{ var('max_recent_orders') }}
    )
{% endmacro %}