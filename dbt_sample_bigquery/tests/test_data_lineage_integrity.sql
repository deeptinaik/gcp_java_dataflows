-- Test to ensure data lineage integrity between staging and marts
WITH staging_totals AS (
    SELECT 
        customer_id,
        SUM((items.value:quantity)::NUMBER * (items.value:price)::NUMBER) AS staging_total_spent,
        COUNT(DISTINCT order_id) AS staging_total_orders
    FROM {{ ref('stg_orders') }},
         LATERAL FLATTEN(input => items) AS items
    GROUP BY customer_id
),
mart_totals AS (
    SELECT 
        customer_id,
        total_spent AS mart_total_spent,
        total_orders AS mart_total_orders
    FROM {{ ref('sales_analytics') }}
)
SELECT 
    s.customer_id,
    s.staging_total_spent,
    m.mart_total_spent,
    s.staging_total_orders,
    m.mart_total_orders
FROM staging_totals s
FULL OUTER JOIN mart_totals m ON s.customer_id = m.customer_id
WHERE 
    s.staging_total_spent != m.mart_total_spent
    OR s.staging_total_orders != m.mart_total_orders
    OR s.customer_id IS NULL
    OR m.customer_id IS NULL