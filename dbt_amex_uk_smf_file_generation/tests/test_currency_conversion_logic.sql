-- Test that currency conversion logic is working correctly
-- This validates the conversion rates from CurrencyConversion.java

SELECT *
FROM {{ ref('amex_uk_smf_output') }}
WHERE currency_code != settlement_currency
  AND conversion_rate = 1.0  -- Should not be 1.0 if currencies are different
  AND currency_code IN ('USD', 'EUR', 'CAD', 'AUD', 'JPY', 'CHF', 'SEK', 'NOK', 'DKK')