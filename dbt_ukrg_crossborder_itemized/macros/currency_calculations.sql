{% macro calculate_expected_settled_amount(payment_method, expected_settled_amount, settlement_currency_exponent) %}
    {{ safe_cast(
        "CASE 
            WHEN " ~ payment_method ~ " = 'mpaynet' THEN 
                ROUND(CAST(" ~ expected_settled_amount ~ " AS NUMERIC), 2) * POWER(CAST(10 AS NUMERIC), CAST(" ~ settlement_currency_exponent ~ " AS NUMERIC))
            ELSE 
                CASE 
                    WHEN REGEXP_LIKE(CAST(" ~ expected_settled_amount ~ " AS STRING), '\\.') THEN
                        CAST(CONCAT(
                            SPLIT_PART(CAST(" ~ expected_settled_amount ~ " AS STRING), '.', 1),
                            '.',
                            SUBSTR(SPLIT_PART(CAST(" ~ expected_settled_amount ~ " AS STRING), '.', 2), 1, 4)
                        ) AS NUMERIC)
                    ELSE " ~ expected_settled_amount ~ "
                END * POWER(CAST(10 AS NUMERIC), CAST(" ~ settlement_currency_exponent ~ " AS NUMERIC))
        END", 
        'NUMERIC'
    ) }}
{% endmacro %}

{% macro calculate_amount(payment_method, amount, currency_exponent) %}
    {{ safe_cast(
        "CASE 
            WHEN " ~ payment_method ~ " = 'mpaynet' THEN 
                ROUND(CAST(" ~ amount ~ " AS NUMERIC), 2) * POWER(CAST(10 AS NUMERIC), CAST(" ~ currency_exponent ~ " AS NUMERIC))
            ELSE 
                CASE 
                    WHEN REGEXP_LIKE(CAST(" ~ amount ~ " AS STRING), '\\.') THEN
                        CAST(CONCAT(
                            SPLIT_PART(CAST(" ~ amount ~ " AS STRING), '.', 1),
                            '.',
                            SUBSTR(SPLIT_PART(CAST(" ~ amount ~ " AS STRING), '.', 2), 1, 4)
                        ) AS NUMERIC)
                    ELSE " ~ amount ~ "
                END * POWER(CAST(10 AS NUMERIC), CAST(" ~ currency_exponent ~ " AS NUMERIC))
        END", 
        'NUMERIC'
    ) }}
{% endmacro %}

{% macro format_system_trace_audit_number(sys_audit_nbr_diff, system_trace_audit_number, reference_number_diff) %}
    CASE
        WHEN LENGTH(TRIM(COALESCE(NULLIF(TRIM({{ sys_audit_nbr_diff }}), ''), NULLIF(TRIM({{ system_trace_audit_number }}), ''), NULLIF(TRIM({{ reference_number_diff }}), '')))) < 6 THEN
            {{ left_pad("TRIM(COALESCE(NULLIF(TRIM(" ~ sys_audit_nbr_diff ~ "), ''), NULLIF(TRIM(" ~ system_trace_audit_number ~ "), ''), NULLIF(TRIM(" ~ reference_number_diff ~ "), '')))", 6, '0') }}
        ELSE
            RIGHT(TRIM(COALESCE(NULLIF(TRIM({{ sys_audit_nbr_diff }}), ''), NULLIF(TRIM({{ system_trace_audit_number }}), ''), NULLIF(TRIM({{ reference_number_diff }}), ''), '000000')), 6)
    END
{% endmacro %}

{% macro format_retrieval_ref_number(retrieval_nbr_diff, retrieval_ref_number, tt_original_reference_tranin) %}
    CASE
        WHEN LENGTH(TRIM(COALESCE(NULLIF(TRIM({{ retrieval_nbr_diff }}), ''), NULLIF(TRIM({{ retrieval_ref_number }}), ''), NULLIF(TRIM({{ tt_original_reference_tranin }}), '')))) < 12 THEN
            {{ right_pad("TRIM(COALESCE(NULLIF(TRIM(" ~ retrieval_nbr_diff ~ "), ''), NULLIF(TRIM(" ~ retrieval_ref_number ~ "), ''), NULLIF(TRIM(" ~ tt_original_reference_tranin ~ "), '')))", 12, ' ') }}
        ELSE
            TRIM(COALESCE(NULLIF(TRIM({{ retrieval_nbr_diff }}), ''), NULLIF(TRIM({{ retrieval_ref_number }}), ''), NULLIF(TRIM({{ tt_original_reference_tranin }}), ''), '000000000000'))
    END
{% endmacro %}