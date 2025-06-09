-- Test that customer tier assignment logic works correctly
-- Based on the AddCustomerTierFn.java logic

WITH test_data AS (
  SELECT
    customer_id,
    amount,
    customer_tier,
    CASE
      WHEN amount > {{ var('customer_tier_gold_threshold') }} THEN 'GOLD'
      WHEN amount > {{ var('customer_tier_silver_threshold') }} THEN 'SILVER'
      ELSE 'BRONZE'
    END AS expected_tier
  FROM {{ ref('int_purchase_events_enriched') }}
)

SELECT
  customer_id,
  amount,
  customer_tier,
  expected_tier
FROM test_data
WHERE customer_tier != expected_tier