{{ config(
    materialized='ephemeral'
) }}

-- Intermediate model: Sales with aggregated items per order
-- Converts BigQuery ARRAY_AGG(STRUCT()) to Snowflake ARRAY_AGG(OBJECT_CONSTRUCT())
SELECT
    order_id,
    customer_id,
    order_date,
    {{ array_agg_struct([
        {'name': 'product_id', 'expr': 'product_id'},
        {'name': 'quantity', 'expr': 'quantity'},
        {'name': 'price', 'expr': 'price'}
    ]) }} AS items
FROM {{ ref('stg_orders') }}
GROUP BY
    order_id, customer_id, order_date