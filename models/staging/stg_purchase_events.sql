{{
  config(
    materialized='view',
    description='Staging table for raw purchase events from PubSub/Snowflake'
  )
}}

-- Raw purchase events from the ingestion layer
-- Equivalent to PubSub topic: projects/your-project/topics/purchase-events

SELECT
    customer_id,
    amount,
    status,
    event_timestamp,
    _loaded_at,
    _source_file
FROM {{ source('raw_data', 'purchase_events') }}
WHERE _loaded_at IS NOT NULL