{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Currency rates staging
-- Prepares currency exchange rates for conversion logic
SELECT 
    from_currency_code,
    to_currency_code,
    exchange_rate,
    effective_date,
    active_flag,
    
    -- Create a composite key for easier lookups
    CONCAT(from_currency_code, '_', to_currency_code) AS currency_pair_key
    
FROM {{ source('transformed_layer', 'dim_currency_rates') }}

WHERE 
    active_flag = '{{ var("active_flag") }}'
    AND exchange_rate > 0
    AND from_currency_code IS NOT NULL
    AND to_currency_code IS NOT NULL
    AND effective_date IS NOT NULL