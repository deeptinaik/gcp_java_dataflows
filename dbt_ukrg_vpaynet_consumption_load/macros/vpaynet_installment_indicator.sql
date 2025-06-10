{% macro get_vpaynet_installment_indicator(vpaynet_installment_fee_detail_sk_vp) %}
  CASE 
    WHEN {{ vpaynet_installment_fee_detail_sk_vp }} != {{ var('default_sk_value') }} 
         AND {{ vpaynet_installment_fee_detail_sk_vp }} IS NOT NULL 
    THEN 'Y' 
    ELSE 'N' 
  END
{% endmacro %}