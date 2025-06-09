{{
  config(
    materialized='view',
    description='Staging table for raw transaction data from PubSub/Snowflake'
  )
}}

-- Raw transactions from the ingestion layer  
-- Equivalent to PubSub topic: projects/your-project/topics/transactions

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_timestamp,
    location,
    _loaded_at,
    _source_file
FROM {{ source('raw_data', 'transactions') }}
WHERE _loaded_at IS NOT NULL