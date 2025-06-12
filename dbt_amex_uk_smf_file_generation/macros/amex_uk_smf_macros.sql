-- Currency conversion macro
-- Replicates the CurrencyConversion.java logic

{% macro convert_currency(transaction_amount, from_currency, to_currency) %}
    
    -- If same currency, no conversion needed
    CASE 
        WHEN {{ from_currency }} = {{ to_currency }} THEN {{ transaction_amount }}
        
        -- Mock exchange rates (in real implementation, would join with currency rates table)
        WHEN {{ from_currency }} = 'USD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' 
            THEN {{ transaction_amount }} * 0.79
        WHEN {{ from_currency }} = 'EUR' AND {{ to_currency }} = '{{ var("gbp_currency") }}' 
            THEN {{ transaction_amount }} * 0.86
        WHEN {{ from_currency }} = '{{ var("gbp_currency") }}' AND {{ to_currency }} = 'USD' 
            THEN {{ transaction_amount }} * 1.27
        WHEN {{ from_currency }} = '{{ var("gbp_currency") }}' AND {{ to_currency }} = 'EUR' 
            THEN {{ transaction_amount }} * 1.16
        WHEN {{ from_currency }} = 'CAD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' 
            THEN {{ transaction_amount }} * 0.58
        WHEN {{ from_currency }} = 'AUD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' 
            THEN {{ transaction_amount }} * 0.52
        WHEN {{ from_currency }} = 'JPY' AND {{ to_currency }} = '{{ var("gbp_currency") }}' 
            THEN {{ transaction_amount }} * 0.0054
        
        -- Default rate for unsupported conversions
        ELSE {{ transaction_amount }}
    END

{% endmacro %}


-- Get exchange rate macro
-- Replicates the getExchangeRate method from CurrencyConversion.java

{% macro get_exchange_rate(from_currency, to_currency) %}
    
    CASE 
        WHEN {{ from_currency }} = {{ to_currency }} THEN 1.0
        
        -- Mock exchange rates
        WHEN {{ from_currency }} = 'USD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.79
        WHEN {{ from_currency }} = 'EUR' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.86
        WHEN {{ from_currency }} = '{{ var("gbp_currency") }}' AND {{ to_currency }} = 'USD' THEN 1.27
        WHEN {{ from_currency }} = '{{ var("gbp_currency") }}' AND {{ to_currency }} = 'EUR' THEN 1.16
        WHEN {{ from_currency }} = 'CAD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.58
        WHEN {{ from_currency }} = 'AUD' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.52
        WHEN {{ from_currency }} = 'JPY' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.0054
        WHEN {{ from_currency }} = 'CHF' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.87
        WHEN {{ from_currency }} = 'SEK' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.075
        WHEN {{ from_currency }} = 'NOK' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.072
        WHEN {{ from_currency }} = 'DKK' AND {{ to_currency }} = '{{ var("gbp_currency") }}' THEN 0.115
        
        -- Default rate
        ELSE 1.0
    END

{% endmacro %}


-- Format amount macro
-- Replicates the formatAmount method from FileFormatting.java

{% macro format_amount(amount_value) %}
    
    CASE 
        WHEN {{ amount_value }} IS NULL THEN '0.00'
        ELSE LPAD(CAST(ROUND({{ amount_value }}, 2) AS VARCHAR), 10, '0')
    END

{% endmacro %}


-- Format string macro
-- Replicates the formatString method from FileFormatting.java

{% macro format_string(string_value, max_length) %}
    
    CASE 
        WHEN {{ string_value }} IS NULL THEN ''
        WHEN LENGTH(TRIM({{ string_value }})) > {{ max_length }} 
            THEN LEFT(TRIM({{ string_value }}), {{ max_length }})
        ELSE TRIM({{ string_value }})
    END

{% endmacro %}


-- Mask card number macro
-- Replicates the maskCardNumber method from FileFormatting.java

{% macro mask_card_number(card_number) %}
    
    CASE 
        WHEN {{ card_number }} IS NULL THEN '***************'
        WHEN LENGTH({{ card_number }}) >= 4 
            THEN CONCAT('***********', RIGHT({{ card_number }}, 4))
        ELSE '***************'
    END

{% endmacro %}