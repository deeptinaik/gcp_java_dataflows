{{
    config(
        materialized='table',
        schema='staging'
    )
}}

-- Source reference for dimension_merch_info table
-- This model is not needed as we use sources.yml configuration

SELECT 1 AS placeholder -- This model can be removed