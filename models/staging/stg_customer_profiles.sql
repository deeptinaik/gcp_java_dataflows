{{
  config(
    materialized='view',
    description='Staging table for customer profile lookup data'
  )
}}

-- Customer profiles for fraud detection enrichment
-- Equivalent to BigQuery table: your-project:dataset.customer_profiles

SELECT
    customer_id,
    avg_amount,
    total_transactions,
    account_created_date,
    risk_score,
    customer_tier,
    _loaded_at
FROM {{ source('raw_data', 'customer_profiles') }}
WHERE _loaded_at IS NOT NULL