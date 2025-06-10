-- Business logic macros for UKRG LCOT UID Auth Dif matching

{# Generate transaction ID trimmed version - removes last character if length > 4 #}
{% macro transaction_id_trim_last_char(transaction_id) %}
  CASE 
    WHEN LENGTH(LTRIM({{ regexp_replace('TRIM(' ~ transaction_id ~ ')', '[^a-zA-Z0-9]+', '') }}, '0')) > 4 
    THEN UPPER(SUBSTR(LTRIM({{ regexp_replace('TRIM(' ~ transaction_id ~ ')', '[^a-zA-Z0-9]+', '') }}, '0'), 1, LENGTH(LTRIM({{ regexp_replace('TRIM(' ~ transaction_id ~ ')', '[^a-zA-Z0-9]+', '') }}, '0')) - 1))
    ELSE UPPER(COALESCE(LTRIM({{ regexp_replace('TRIM(' ~ transaction_id ~ ')', '[^a-zA-Z0-9]+', '') }}, '0'), ''))
  END
{% endmacro %}

{# Global TRID generation for authorization records #}
{% macro generate_global_trid_auth(amount, network_ref, merchant_number, tran_date, global_trid) %}
  COALESCE(
    {{ global_trid }},
    {{ sha512_base64('CONCAT(COALESCE(' ~ amount ~ ', 0), TRIM(SUBSTRING(COALESCE(TRIM(' ~ network_ref ~ '), \'\'), 1, 15)), LTRIM(TRIM(' ~ merchant_number ~ '), \'0\'), ' ~ tran_date ~ ')') }}
  )
{% endmacro %}

{# Terminal ID comparison logic - handles empty and null cases #}
{% macro terminal_id_match(terminal_id_t, terminal_id_a) %}
  ({{ terminal_id_t }} = {{ terminal_id_a }} 
   OR (TRIM({{ terminal_id_t }}) = '' AND TRIM({{ terminal_id_a }}) <> '') 
   OR (TRIM({{ terminal_id_t }}) = '' AND TRIM({{ terminal_id_a }}) = ''))
{% endmacro %}

{# Amount comparison with rounding tolerance #}
{% macro amount_equals_with_tolerance(amount1, amount2) %}
  ({{ amount1 }} = {{ amount2 }} OR ROUND({{ amount1 }}) = ROUND({{ amount2 }}))
{% endmacro %}

{# LCOT GUID Key SK selection logic #}
{% macro select_lcot_guid_key_sk(trans_sk, auth_sk, lcot_guid_key_sk_trans, lcot_guid_key_sk_auth) %}
  CASE 
    WHEN {{ trans_sk }} <> '-1' AND {{ auth_sk }} <> '-1' THEN COALESCE({{ lcot_guid_key_sk_trans }}, {{ lcot_guid_key_sk_auth }})
    WHEN {{ trans_sk }} <> '-1' THEN {{ lcot_guid_key_sk_trans }}
    WHEN {{ auth_sk }} <> '-1' THEN {{ lcot_guid_key_sk_auth }}
    ELSE {{ generate_uuid() }}
  END
{% endmacro %}

{# Date range validation for transaction matching #}
{% macro date_range_check(tran_date, auth_date, start_interval, end_interval) %}
  {{ auth_date }} BETWEEN {{ date_add(tran_date, start_interval, 'DAY') }} AND {{ date_add(tran_date, end_interval, 'DAY') }}
{% endmacro %}

{# Complex field coalescing for merged records #}
{% macro coalesce_fields(field_name, trans_value, auth_value, default_value = "''") %}
  COALESCE({{ trans_value }}, {{ auth_value }}, {{ default_value }})
{% endmacro %}

{# Authorization response code validation #}
{% macro auth_response_code_valid(resp_code, record_type, rec_type, excep_reason_code) %}
  (
    (
      COALESCE({{ resp_code }}, 0) < 50 
      AND COALESCE({{ record_type }}, '') <> 'mpg_s'
      AND (COALESCE({{ rec_type }}, '') <> '21' OR COALESCE({{ excep_reason_code }}, '') < '500' OR COALESCE({{ excep_reason_code }}, '') > '511')
    )
    OR
    (
      COALESCE({{ record_type }}, '') = 'mpg_s'
      AND COALESCE({{ resp_code }}, 0) <> 05
    )
  )
{% endmacro %}

{# Transaction ID substring generation for matching #}
{% macro transaction_id_variants(transaction_id) -%}
  {%- set variants = {
    'first_4': 'SUBSTR(' ~ transaction_id ~ ', 1, 4)',
    'last_4': 'SUBSTR(' ~ transaction_id ~ ', LENGTH(' ~ transaction_id ~ ') - 3)',
    'last_6_first_4': 'SUBSTR(SUBSTR(TRIM(' ~ transaction_id ~ '), LENGTH(TRIM(' ~ transaction_id ~ ')) - 5), 1, 4)',
    'first_letter': 'SUBSTR(' ~ transaction_id ~ ', 1, 1)'
  } -%}
  {{ return(variants) }}
{%- endmacro %}

{# Generate window function partitions for transaction ranking #}
{% macro transaction_ranking_partition(merchant, amount, transaction_id, auth_code, tran_date, tran_time, card_number, order_by_fields) %}
  ROW_NUMBER() OVER(
    PARTITION BY 
      {{ merchant }}, 
      CAST({{ amount }} AS STRING), 
      {{ transaction_id }}, 
      {{ auth_code }}, 
      {{ tran_date }}, 
      {{ tran_time }}, 
      {{ card_number }}
    ORDER BY {{ order_by_fields }}
  )
{% endmacro %}

{# Authorization ranking partition with terminal and sequence #}
{% macro auth_ranking_partition(merchant, amount, network_ref, auth_code, tran_date, tran_time, card_number, order_by_fields) %}
  ROW_NUMBER() OVER(
    PARTITION BY 
      {{ merchant }}, 
      CAST({{ amount }} AS STRING), 
      {{ network_ref }}, 
      {{ auth_code }}, 
      {{ tran_date }}, 
      {{ tran_time }}, 
      {{ card_number }}
    ORDER BY {{ order_by_fields }}
  )
{% endmacro %}