-- Sample: Analyze sales data with advanced transformations
WITH sales AS (
  SELECT
    order_id,
    customer_id,
    order_date,
    ARRAY_AGG(STRUCT(product_id, quantity, price)) AS items
  FROM
    `project.dataset.orders`
  GROUP BY
    order_id, customer_id, order_date
),
customer_totals AS (
  SELECT
    customer_id,
    SUM(quantity * price) AS total_spent,
    COUNT(DISTINCT order_id) AS total_orders
  FROM
    sales, UNNEST(items)
  GROUP BY
    customer_id
),
ranked_orders AS (
  SELECT
    order_id,
    customer_id,
    order_date,
    SUM(quantity * price) AS order_total,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
  FROM
    sales, UNNEST(items)
  GROUP BY
    order_id, customer_id, order_date
)
SELECT
  c.customer_id,
  c.total_spent,
  c.total_orders,
  ARRAY_AGG(STRUCT(r.order_id, r.order_total, r.order_date) ORDER BY r.order_date DESC LIMIT 3) AS last_3_orders,
  CASE
    WHEN c.total_spent > 10000 THEN 'VIP'
    WHEN c.total_spent > 5000 THEN 'Preferred'
    ELSE 'Standard'
  END AS customer_tier
FROM
  customer_totals c
JOIN
  ranked_orders r
ON
  c.customer_id = r.customer_id
WHERE
  r.order_rank <= 3
GROUP BY
  c.customer_id, c.total_spent, c.total_orders
ORDER BY
  c.total_spent DESC