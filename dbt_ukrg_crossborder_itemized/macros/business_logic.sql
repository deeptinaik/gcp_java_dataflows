{% macro classify_domestic_international(country_code, alpha_currency_code, charge_type) %}
    CASE
        WHEN {{ country_code }} = 'GB' AND {{ alpha_currency_code }} = 'GBP' AND ({{ charge_type }} LIKE '18K%' OR {{ charge_type }} LIKE '12K%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'IE' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18M%' OR {{ charge_type }} LIKE '12E%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'DE' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18G%' OR {{ charge_type }} LIKE '12G%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'NL' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18H%' OR {{ charge_type }} LIKE '12H%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'IT' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18T%' OR {{ charge_type }} LIKE '12T%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'ES' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18S%' OR {{ charge_type }} LIKE '12S%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'FR' AND {{ alpha_currency_code }} = 'EUR' AND ({{ charge_type }} LIKE '18F%' OR {{ charge_type }} LIKE '12F%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'NO' AND ({{ charge_type }} LIKE '18N%' OR {{ charge_type }} LIKE '12N%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'SE' AND ({{ charge_type }} LIKE '18F%' OR {{ charge_type }} LIKE '12W%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'DK' AND ({{ charge_type }} LIKE '18D%' OR {{ charge_type }} LIKE '12D%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'BE' AND ({{ charge_type }} LIKE '18L%' OR {{ charge_type }} LIKE '12L%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'FI' AND ({{ charge_type }} LIKE '18J%' OR {{ charge_type }} LIKE '12J%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'AT' AND ({{ charge_type }} LIKE '12R%' OR {{ charge_type }} LIKE '18R%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'PL' AND ({{ charge_type }} LIKE '13P%' OR {{ charge_type }} LIKE '18P%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'CH' AND ({{ charge_type }} LIKE '12Q%' OR {{ charge_type }} LIKE '18Q%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'CZ' AND ({{ charge_type }} LIKE '12Z%' OR {{ charge_type }} LIKE '18Z%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'RO' AND ({{ charge_type }} LIKE '13M%' OR {{ charge_type }} LIKE '18B%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'HU' AND ({{ charge_type }} LIKE '12Y%' OR {{ charge_type }} LIKE '18Y%') THEN 'DOMESTIC'
        WHEN {{ country_code }} = 'PT' AND ({{ charge_type }} LIKE '12P%') THEN 'DOMESTIC'
        ELSE 'INTERNATIONAL'
    END
{% endmacro %}

{% macro build_hierarchy(corp_diff, region_diff, principal_diff, associate_diff, chain_diff) %}
    CONCAT(
        COALESCE({{ corp_diff }}, ''),
        CASE WHEN {{ corp_diff }} IS NULL THEN '' ELSE '-' END,
        COALESCE({{ region_diff }}, ''),
        CASE WHEN {{ region_diff }} IS NULL THEN '' ELSE '-' END,
        COALESCE({{ principal_diff }}, ''),
        CASE WHEN {{ principal_diff }} IS NULL THEN '' ELSE '-' END,
        COALESCE({{ associate_diff }}, ''),
        CASE WHEN {{ associate_diff }} IS NULL THEN '' ELSE '-' END,
        COALESCE({{ chain_diff }}, '')
    )
{% endmacro %}

{% macro build_order_id(transaction_date_diff, sys_audit_nbr_diff, system_trace_audit_number, reference_number_diff, retrieval_nbr_diff, retrieval_ref_number, tt_original_reference_tranin) %}
    CONCAT(
        {{ format_date_snowflake("COALESCE(" ~ transaction_date_diff ~ ", CURRENT_DATE())", 'MMDD') }},
        {{ format_system_trace_audit_number(sys_audit_nbr_diff, system_trace_audit_number, reference_number_diff) }},
        {{ format_retrieval_ref_number(retrieval_nbr_diff, retrieval_ref_number, tt_original_reference_tranin) }}
    )
{% endmacro %}

{% macro build_merchant_aggregate_reference(processing_date, geographical_region, acquirer_bin, merchant_number) %}
    CONCAT(
        COALESCE({{ safe_cast(format_date_snowflake("DATEADD(day, 1, " ~ parse_date_snowflake(processing_date, 'YYYYMMDD') ~ ")", 'YYDDD'), 'STRING') }}, ''),
        COALESCE({{ geographical_region }}, ''),
        COALESCE({{ acquirer_bin }}, ''),
        COALESCE({{ merchant_number }}, ''),
        '0008'
    )
{% endmacro %}

{% macro build_bin_aggregate_reference(processing_date, geographical_region, acquirer_bin) %}
    CONCAT(
        COALESCE({{ safe_cast(format_date_snowflake("DATEADD(day, 1, " ~ parse_date_snowflake(processing_date, 'YYYYMMDD') ~ ")", 'YYDDD'), 'STRING') }}, ''),
        COALESCE({{ geographical_region }}, ''),
        COALESCE({{ acquirer_bin }}, ''),
        '0008'
    )
{% endmacro %}