{% macro handle_null_vpaynet_sk(vpaynet_installment_fee_detail_sk) %}
  CASE 
    WHEN {{ vpaynet_installment_fee_detail_sk }} IS NULL 
    THEN {{ var('default_sk_value') }}
    ELSE {{ vpaynet_installment_fee_detail_sk }}
  END
{% endmacro %}