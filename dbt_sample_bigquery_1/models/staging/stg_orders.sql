/*
  Staging model for orders data
  Converts BigQuery source table to Snowflake staging format
  Replaces: `project.dataset.orders` reference in original BigQuery
*/

{{ config(
    materialized='view',
    schema='staging_layer'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    product_id,
    quantity,
    price,
    quantity * price AS order_item_total,
    {{ generate_current_timestamp() }} AS etl_load_timestamp
FROM {{ source('raw_sales_data', 'orders') }}

-- Apply basic data quality filters
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity > 0
  AND price > 0