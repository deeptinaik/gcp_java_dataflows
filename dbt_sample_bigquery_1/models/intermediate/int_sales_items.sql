/*
  Intermediate model: Sales with aggregated items
  Converts BigQuery ARRAY_AGG(STRUCT(...)) to Snowflake equivalent
  Replaces the 'sales' CTE from original BigQuery query
*/

{{ config(
    materialized='ephemeral'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    {{ array_agg_struct_snowflake('product_id', 'quantity', 'price') }} AS items_json
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, 
    customer_id, 
    order_date