#!/bin/bash
global_var=""
etlbatchid="$1"
source_table="$2"

bq_run()
{
	QUERY=$1 
	jobID="master_transaction_detail_vpaynet_uk-"`date '+%Y%m%d%H%M%S%3N'`
echo -e "\n****************** Query To Execute: ${QUERY} ******************\n"
echo -e "\n****************** Command To Execute: bq query --use_legacy_sql=FALSE ${QUERY} ******************\n"

dry_run_output=`bq query --use_legacy_sql=FALSE --dry_run "${QUERY}"`

if [ $? -eq 0 ]
then
	echo -e "\n****************** $dry_run_output ******************\n"
	
	execution_output=`bq query --job_id=$jobID --use_legacy_sql=FALSE "${QUERY}"`
	
	if [ $? -eq 0 ]
	then
		echo -e "\n****************** Query Ran Successfully ******************\n"
		
		number_of_rows_affected=`bq show --format=prettyjson -j $jobID | grep "numDmlAffectedRows" | head -n 1 | cut -d':' -f2 |sed 's/[\", ]//g'`
		echo "Number Of Rows Affected: $number_of_rows_affected"
		global_var=$global_var$2" Number Of Rows Affected: $number_of_rows_affected"

	else
		echo -ne "{\\\"jobid\\\" : \\\"$jobID\\\",\\\"error\\\" : \\\"$execution_output\\\"}"|tr -d '\n'|tr "'"		"*"
		exit 2
	fi
else
	echo -e "\n****************** Query Validation Failed.... Cause: $dry_run_output ******************\n"
	exit 1
fi
}

vpaynet_consumption_load="
MERGE consumption_layer.wwmaster_transaction_detail AS TARGET USING ( SELECT A.etlbatchid, B.trans_sk, A.uk_trans_ft_sk, CASE WHEN src.vpaynet_installment_fee_detail_sk IS NULL THEN '-1' ELSE src.vpaynet_installment_fee_detail_sk END AS vpaynet_installment_fee_detail_sk, CASE WHEN B.vpaynet_installment_fee_detail_sk_vp <>'-1' AND B.vpaynet_installment_fee_detail_sk_vp IS NOT NULL THEN 'Y' ELSE 'N' END AS vp_vpaynet_installment_ind, src.arn AS vp_arn, src.merchant_number AS vp_merchant_number, src.vpaynet_installment_plan_id AS vp_vpaynet_installment_plan_id, src.installment_plan_name AS vp_installment_plan_name, src.installment_transaction_status AS vp_installment_transaction_status, src.plan_frequency AS vp_plan_frequency, src.number_of_installments AS vp_number_of_installments, src.plan_promotion_code AS vp_plan_promotion_code, src.plan_acceptance_created_date_time AS vp_plan_acceptance_created_date_time, src.transaction_amount AS vp_transaction_amount, src.transaction_currency_code AS vp_transaction_currency_code, src.clearing_amount AS vp_clearing_amount, src.clearing_currency_code AS vp_clearing_currency_code, src.clearing_date_time AS vp_clearing_date_time, src.cancelled_amount AS vp_cancelled_amount, src.cancelled_currency_code AS vp_cancelled_currency_code, src.derived_flag AS vp_derived_flag, src.cancelled_date_time AS vp_cancelled_date_time, src.installment_funding_fee_amount AS vp_installment_funding_fee_amount, src.installment_funding_fee_currency_code AS vp_installment_funding_fee_currency_code, src.installments_service_fee_eligibility_amount AS vp_installments_service_fee_eligibility_amount, src.installments_service_fee_eligibility_currency_code AS vp_installments_service_fee_eligibility_currency_code, src.funding_type AS vp_funding_type, src.etlbatchid AS vp_etlbatchid FROM (select * from transformed_layer.lotr_uid_key_ukrg where trans_sk <>'-1' QUALIFY(ROW_NUMBER() OVER (PARTITION BY trans_sk ORDER BY vpaynet_installment_fee_detail_sk_vp DESC,etlbatchid DESC)=1) )B INNER JOIN (select uk_trans_ft_sk,etlbatchid from transformed_layer.vw_ukrg_trans_fact  WHERE PARSE_DATE('%Y%m%d',SUBSTR(CAST(etlbatchid AS string),1,8) ) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 day) AND CURRENT_DATE() QUALIFY(ROW_NUMBER() OVER (PARTITION BY uk_trans_ft_sk ORDER BY etlbatchid DESC)=1) )A ON A.uk_trans_ft_sk=B.trans_sk LEFT JOIN transformed_layer.vpaynet_installment_fee_detail_daily_uk src ON src.vpaynet_installment_fee_detail_sk=B.vpaynet_installment_fee_detail_sk_vp ) AS DATA ON target.transaction_detail_sk = DATA.uk_trans_ft_sk WHEN MATCHED THEN UPDATE SET target.vp_vpaynet_installment_fee_detail_sk = DATA.vpaynet_installment_fee_detail_sk, target.vp_vpaynet_installment_ind = DATA.vp_vpaynet_installment_ind, target.vp_arn = DATA.vp_arn, target.vp_merchant_number = DATA.vp_merchant_number, target.vp_vpaynet_installment_plan_id = DATA.vp_vpaynet_installment_plan_id, target.vp_installment_plan_name = DATA.vp_installment_plan_name, target.vp_installment_transaction_status = DATA.vp_installment_transaction_status, target.vp_plan_frequency = DATA.vp_plan_frequency, target.vp_number_of_installments = DATA.vp_number_of_installments, target.vp_plan_promotion_code = DATA.vp_plan_promotion_code, target.vp_plan_acceptance_created_date_time = DATA.vp_plan_acceptance_created_date_time, target.vp_transaction_amount = DATA.vp_transaction_amount, target.vp_transaction_currency_code = DATA.vp_transaction_currency_code, target.vp_clearing_amount = DATA.vp_clearing_amount, target.vp_clearing_currency_code = DATA.vp_clearing_currency_code, target.vp_clearing_date_time = DATA.vp_clearing_date_time, target.vp_cancelled_amount = DATA.vp_cancelled_amount, target.vp_cancelled_currency_code = DATA.vp_cancelled_currency_code, target.vp_derived_flag = DATA.vp_derived_flag, target.vp_cancelled_date_time = DATA.vp_cancelled_date_time, target.vp_installment_funding_fee_amount = DATA.vp_installment_funding_fee_amount, target.vp_installment_funding_fee_currency_code = DATA.vp_installment_funding_fee_currency_code, target.vp_installments_service_fee_eligibility_amount = DATA.vp_installments_service_fee_eligibility_amount, target.vp_installments_service_fee_eligibility_currency_code = DATA.vp_installments_service_fee_eligibility_currency_code, target.vp_funding_type = DATA.vp_funding_type, target.vp_etlbatchid = DATA.vp_etlbatchid
"
	

echo "*************** Calling generic function for bq tables to run ********************"
bq_run "${vpaynet_consumption_load}" 

#As the number of queries increases please add function call in the above fashion.


