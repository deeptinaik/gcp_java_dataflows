-- Test to validate that vpaynet installment indicator logic is correct
-- This test ensures that the CASE logic from the original BigQuery MERGE is preserved

SELECT 
  transaction_detail_sk,
  vp_vpaynet_installment_fee_detail_sk,
  vp_vpaynet_installment_ind,
  -- Expected indicator based on business rules
  CASE 
    WHEN vp_vpaynet_installment_fee_detail_sk != '-1' 
         AND vp_vpaynet_installment_fee_detail_sk IS NOT NULL 
    THEN 'Y' 
    ELSE 'N' 
  END AS expected_indicator
FROM {{ ref('wwmaster_transaction_detail_vpaynet_update') }}
WHERE vp_vpaynet_installment_ind != (
  CASE 
    WHEN vp_vpaynet_installment_fee_detail_sk != '-1' 
         AND vp_vpaynet_installment_fee_detail_sk IS NOT NULL 
    THEN 'Y' 
    ELSE 'N' 
  END
)