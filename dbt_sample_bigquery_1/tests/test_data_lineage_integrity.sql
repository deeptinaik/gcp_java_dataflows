/*
  Test to validate data lineage integrity between staging and marts
  Ensures no customers are lost in transformation process
*/

WITH staging_customers AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_orders') }}
),
marts_customers AS (
    SELECT DISTINCT customer_id 
    FROM {{ ref('customer_analysis') }}
),
missing_customers AS (
    SELECT s.customer_id
    FROM staging_customers s
    LEFT JOIN marts_customers m ON s.customer_id = m.customer_id
    WHERE m.customer_id IS NULL
)

SELECT * FROM missing_customers