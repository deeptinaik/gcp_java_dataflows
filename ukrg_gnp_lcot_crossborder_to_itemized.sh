#!/bin/bash
set -o pipefail

global_var=""
lcot_ETLBATCHID=$1

if [ $# -ne 1 ]
then
    echo -e "Parameters incorrect expected parameters for variable lcot_ETLBATCHID\n"
    exit 1
fi

bq_run()
{
	QUERY=$1 
	
	jobID="mybank-itemized-$2-"`date '+%Y%m%d%H%M%S%3N'`

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
		global_var=$global_var$2" Number Of Rows Affected: $number_of_rows_affected "

	else
		echo -ne "{\\\"jobid\\\" : \\\"$jobID\\\",\\\"error\\\" : \\\"$execution_output\\\"}"|tr -d '\n'|tr "'"		"*"
		exit 2
	fi
else
	echo -e "\n****************** Query Validation Failed.... Cause: $dry_run_output ******************\n"
	exit 1
fi
}

#Crossborder
merge_itemized3="MERGE
  transformed_layer_commplat.mybank_itmz_dtl_uk target
USING
  (
  SELECT
    * EXCEPT (amount,
      expected_settled_amount,system_trace_audit_number,retrieval_ref_number),
    CONCAT( ifnull(SAFE_CAST(FORMAT_DATE('%C%y%j', DATE_ADD(parse_DATE('%Y%m%d',
                processing_date), INTERVAL 1 day) ) AS string),
        ''),ifnull(geographical_region,
        ''),ifnull(acquirer_bin,
        ''),ifnull(merchant_number,
        ''),'0008') AS merchant_aggregate_reference,
    CONCAT( ifnull(SAFE_CAST(FORMAT_DATE('%C%y%j', DATE_ADD(parse_DATE('%Y%m%d',
                processing_date), INTERVAL 1 day) ) AS string),
        ''),ifnull(geographical_region,
        ''),ifnull(acquirer_bin,
        ''),'0008') AS bin_aggregate_reference,
    CONCAT(ifnull(merchant_number,
        ''), ifnull(mybank_itemized_detail_sk,
        '') ) AS merchant_sequence,
    SAFE_CAST(
      CASE
        WHEN payment_method= 'mpaynet' THEN ROUND(CAST(expected_settled_amount AS numeric),2 )* POW(CAST(10 AS numeric), CAST(settlement_currency_exponent AS numeric))
      ELSE
      IF(REGEXP_CONTAINS(cast(expected_settled_amount as string), r'\.'),cast(CONCAT(split(cast(expected_settled_amount as string),'.')[safe_offset(0)],'.',substr(split(cast(expected_settled_amount as string),'.')[safe_offset(1)],0,4)) as NUMERIC),expected_settled_amount) * POW(CAST(10 AS numeric), CAST(settlement_currency_exponent AS numeric)) END AS numeric ) AS expected_settled_amount,
    SAFE_CAST(
      CASE
        WHEN payment_method= 'mpaynet' THEN ROUND(CAST(amount AS numeric),2)* POW(CAST(10 AS numeric), CAST(currency_exponent AS numeric))
      ELSE
      IF(REGEXP_CONTAINS(cast(amount as string), r'\.'),cast(CONCAT(split(cast(amount as string),'.')[safe_offset(0)],'.',substr(split(cast(amount as string),'.')[safe_offset(1)],0,4)) as NUMERIC),amount) * POW(CAST(10 AS numeric), CAST(currency_exponent AS numeric))
    END
      AS numeric) AS amount,
    IF
      (LENGTH(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''))) ) <6,
        lpad(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''))),
          6,
          '0'),
        SUBSTR(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''),'000000')),-6,6) ) AS system_trace_audit_number,
    IF
      (LENGTH(TRIM(coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''))) ) <12,
        rpad(TRIM(coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''))),
          12,
          ' '),
        TRIM( coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''),'000000000000')) ) AS retrieval_ref_number,
    CONCAT(REPLACE(SUBSTR(CAST(create_date_time AS string),1,19),' ','T'),'Z') AS event_timestamp,
    CASE
      WHEN country_code='GB' AND alpha_currency_code='GBP' AND ( charge_type LIKE '18K%' OR charge_type LIKE '12K%') THEN 'DOMESTIC'
      WHEN country_code='IE'
    AND alpha_currency_code='EUR'
    AND ( charge_type LIKE '18M%'
      OR charge_type LIKE '12E%') THEN 'DOMESTIC'
      WHEN country_code='DE' AND alpha_currency_code='EUR' AND ( charge_type LIKE '18G%' OR charge_type LIKE '12G%') THEN 'DOMESTIC'
      WHEN country_code='NL'
    AND alpha_currency_code='EUR'
    AND ( charge_type LIKE '18H%'
      OR charge_type LIKE '12H%') THEN 'DOMESTIC'
      WHEN country_code='IT' AND alpha_currency_code='EUR' AND ( charge_type LIKE '18T%' OR charge_type LIKE '12T%') THEN 'DOMESTIC'
      WHEN country_code='ES'
    AND alpha_currency_code='EUR'
    AND ( charge_type LIKE '18S%'
      OR charge_type LIKE '12S%') THEN 'DOMESTIC'
      WHEN country_code='FR' AND alpha_currency_code='EUR' AND ( charge_type LIKE '18F%' OR charge_type LIKE '12F%') THEN 'DOMESTIC'
	  when country_code = 'NO' and ( charge_type like '18N%' or charge_type like '12N%' ) then 'DOMESTIC'
	  when country_code = 'SE' and ( charge_type like '18F%' or charge_type like '12W%' ) then 'DOMESTIC'
	  when country_code = 'DK' and ( charge_type like '18D%' or charge_type like '12D%' ) then 'DOMESTIC'
      when country_code = 'BE' and ( charge_type like '18L%' or charge_type like '12L%' ) then 'DOMESTIC'
      when country_code = 'FI' and ( charge_type like '18J%' or charge_type like '12J%' ) then 'DOMESTIC'
	  
	  WHEN country_code = 'AT' AND ( charge_type like '12R%' or charge_type like '18R%' ) THEN 'DOMESTIC'
	  WHEN country_code = 'PL' AND ( charge_type like '13P%' or charge_type like '18P%' ) THEN 'DOMESTIC'
	  WHEN country_code = 'CH' AND ( charge_type like '12Q%' or charge_type like '18Q%' ) THEN 'DOMESTIC'
	  when country_code = 'CZ' AND ( charge_type like '12Z%' or charge_type like '18Z%' ) THEN 'DOMESTIC'
	  WHEN country_code = 'RO' AND ( charge_type like '13M%' or charge_type like '18B%' ) THEN 'DOMESTIC'
	  WHEN country_code = 'HU' AND ( charge_type like '12Y%' or charge_type like '18Y%' ) THEN 'DOMESTIC'
	  WHEN country_code = 'PT' AND ( charge_type like '12P%' ) THEN 'DOMESTIC'	  

    ELSE
    'INTERNATIONAL'
  END
    AS domestic_international_ind
  FROM (
    SELECT
      generate_uuid() AS mybank_itemized_detail_sk,
      $lcot_ETLBATCHID AS etlbatchid,
      /* TODO: PARAMETERIZE ETLBATCHID*/ current_datetime() AS create_date_time,
      current_datetime() AS update_date_time,
      trans_sk_guid AS trans_sk_guid,
      ifnull(chargeback_sk_chargeback_eod,
        '-1') AS chargeback_sk_chargeback_eod,
      '-1' AS member_balancing_sk,
      '-1' AS vp_mp_rejects_sk,
      '-1' AS mybank_vp_mp_reclass_items_sk,
      '-1' AS exception_action_report_sk,
      'FEE' AS type,
      'TRANSACTION_FEE' AS sub_type,
      'CROSSBORDER_FEE' AS fee_type,
      CASE
        WHEN dim_ctry.mybank_region IS NOT NULL THEN dim_ctry.mybank_geographical_region
      ELSE
      'UK'
    END
      geographical_region,
      'COMPANYNAME' AS mybank_acquirer_id,
      transaction_id_diff AS transaction_id,
      CONCAT(ifnull(corp_diff,
          ''),
      IF
        (corp_diff IS NULL,
          '',
          '-'),ifnull(region_diff,
          ''),
      IF
        (region_diff IS NULL,
          '',
          '-'),ifnull(principal_diff,
          ''),
      IF
        (principal_diff IS NULL,
          '',
          '-'),ifnull(associate_diff,
          ''),
      IF
        (associate_diff IS NULL,
          '',
          '-'),ifnull(chain_diff,
          '') ) AS hierarchy,
      merchant_number_diff AS merchant_number,
      ifnull(CASE
          WHEN payment_method='mpaynet' THEN assc_parm1.mp_ica
          WHEN payment_method='vpaynet' THEN assc_parm1.vpaynet_bin
      END
        ,
        SUBSTR(arn, 2, 6)) AS acquirer_bin,
      CAST(merchant_number_diff AS int64) AS merchant_number_int,
      CONCAT(FORMAT_DATE('%m%d', ifnull (transaction_date_diff, CURRENT_DATE()) ),
    IF
      (LENGTH(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''))) ) <6,
        lpad(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''))),
          6,
          '0'),
        SUBSTR(TRIM(coalesce(nullif(trim(sys_audit_nbr_diff),''),nullif(trim(system_trace_audit_number),''),nullif(trim(reference_number_diff),''),'000000')),-6,6) ),
    IF
      (LENGTH(TRIM(coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''))) ) <12,
        rpad(TRIM(coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''))),
          12,
          ' '),
        TRIM( coalesce(nullif(trim(retrieval_nbr_diff),''),nullif(trim(retrieval_ref_number),''),nullif(trim(tt_original_reference_tranin),''),'000000000000')) ))
      AS order_id,
      CASE
          WHEN mybank_settlement_currency_array like '%'||alpha_currency_code_diff||'%' THEN alpha_currency_code_diff
      ELSE
      ifnull(lcot.settle_alpha_currency_code_tran_eod,
        (
          CASE
            WHEN tt_transaction_amount_tranin IS NOT NULL THEN 'GBP'
        END
          ))
    END
      AS alpha_currency_code,
      CASE
        WHEN payment_method ='mpaynet' THEN 2
      ELSE
      4
    END
      AS currency_exponent,
      CASE
          WHEN mybank_settlement_currency_array like '%'||alpha_currency_code_diff||'%' THEN CAST(transaction_amount_diff AS numeric)*xb_signed_rate
      ELSE
      CAST(ifnull(settlement_currency_amount_tran_eod,
          tt_transaction_amount_tranin) AS numeric)*xb_signed_rate
    END
      AS amount,
      CASE
        WHEN dim_ctry.mybank_region IS NOT NULL THEN dim_ctry.country_code_2a
      ELSE
      'GB'
    END
      country_code,
      NULL AS reference_transaction_id,
      'PROCESSED' AS status,
      NULL AS reason_text,
      payment_method,
      FORMAT_DATE('%Y%m%d', file_date_diff) AS processing_date,
      file_date_diff AS file_date,
      arn,
      CASE
          WHEN mybank_settlement_currency_array like '%'||alpha_currency_code_diff||'%' THEN alpha_currency_code_diff
      ELSE
      ifnull(lcot.settle_alpha_currency_code_tran_eod,
        (
          CASE
            WHEN tt_transaction_amount_tranin IS NOT NULL THEN 'GBP'
        END
          ))
    END
      AS settlement_alpha_currency_code,
      CASE
          WHEN mybank_settlement_currency_array like '%'||alpha_currency_code_diff||'%' THEN CAST(transaction_amount_diff AS numeric)*xb_signed_rate
      ELSE
      CAST(ifnull(settlement_currency_amount_tran_eod,
          tt_transaction_amount_tranin) AS numeric)*xb_signed_rate
    END
      AS expected_settled_amount,
      CASE
        WHEN payment_method ='mpaynet' THEN 2
      ELSE
      4
    END
      AS settlement_currency_exponent,
      FORMAT_DATE( '%Y%m%d', DATE_ADD( file_date_diff, INTERVAL 1 day) ) AS expected_settlement_date,
      1 AS scheme_rate,
      card_type_diff AS card_type,
      card_type_desc AS card_type_desc,
      charge_type_diff AS charge_type,
      charge_type_desc AS charge_type_desc,
      ifnull(fee_program_ind,
        ird) AS interchange_fee_program,
      fee_program_desc AS interchange_fee_program_desc,
      CONCAT(CAST(xb_signed_rate*-100 AS STRING),'%') AS interchange_rate,
	  sys_audit_nbr_diff,
      retrieval_nbr_diff,
      system_trace_audit_number,
      retrieval_ref_number,
      reference_number_diff,
      tt_original_reference_tranin,
	  card_country_code,
	  account_funding_source,
	  card_bin,
	  bin_detail_sk
    FROM (
      SELECT
        DISTINCT '-1' AS chargeback_sk_chargeback_eod,
        merchant_number_diff,
        NULL AS order_nbr_tds,
        alpha_currency_code_diff,
        card_type_diff,
        charge_type_diff,
        transaction_amount_diff,
        transaction_id_diff,
        card_scheme_diff,
        CASE
          WHEN card_scheme_diff='mp' THEN 'mpaynet'
          WHEN card_scheme_diff='vp' THEN 'vpaynet'
      END
        AS payment_method,
        tt_acq_reference_number_tranin AS arn,
        moto_ec_indicator_diff,
        cardholder_id_method_diff,
        trans_sk_guid,
        corp_diff,
        region_diff,
        principal_diff,
        associate_diff,
        chain_diff,
        tran_code_diff,
        file_date_diff,
        tt_acq_reference_number_tranin,
        settle_alpha_currency_code_tran_eod,
        settlement_currency_amount_tran_eod,
        cross_border_indicator_diff,
        transaction_date_diff,
        reference_number_diff,
        tt_original_reference_tranin,
        dimipc.fee_program_ind,
        dimipc.ird,
        tt_transaction_amount_tranin,
        system_trace_audit_number,
        retrieval_ref_number,
		card_country_code,
		account_funding_source,
		card_bin,
		bin_detail_sk,
        dim_card_desc.card_type_desc AS card_type_desc,
        dim_charge_desc.charge_type_desc AS charge_type_desc,
        dimipc.fee_program_desc,
        xb.xb_signed_rate,
        corp_guid,
        region_guid,
		sys_audit_nbr_diff,
        retrieval_nbr_diff
      FROM
        transformed_layer_commplat.temp_data_source_uk_gnp_sup_tbl_ltr a
      INNER JOIN
        transformed_layer.dim_xb_rt_mtx_mybank xb
      ON
        corp_diff=xb.corp
        AND card_scheme_diff = xb.card_scheme
        AND ( region_diff IN UNNEST(xb.region_inclusion_array)
          OR ARRAY_LENGTH(xb.region_inclusion_array) = 0 )
        AND ( region_diff NOT IN UNNEST(xb.region_exclusion_array) )
        AND ( card_type_diff = xb.card_type_inclusion_regexp
          OR xb.card_type_inclusion_regexp IS NULL)
        AND ( card_type_diff != xb.card_type_exclusion_regexp)
        AND ( REGEXP_CONTAINS(charge_type_diff, xb.charge_type_inclusion_regexp )
          OR coalesce(TRIM(xb.charge_type_inclusion_regexp),
            '') = '' )
        AND ( NOT ( REGEXP_CONTAINS(charge_type_diff, xb.charge_type_exclusion_regexp))
          OR coalesce(TRIM(xb.charge_type_exclusion_regexp),
            '') = '' ) AND( alpha_currency_code_diff IN UNNEST(xb.currency_code_inclusion_array)
          OR ARRAY_LENGTH(xb.currency_code_inclusion_array) = 0 )
        AND ( alpha_currency_code_diff NOT IN UNNEST(xb.currency_code_exclusion_array) )
        AND ( tran_code_diff IN UNNEST(xb.tran_code_inclusion_array)
          OR ARRAY_LENGTH(xb.tran_code_inclusion_array) = 0 )
        AND ( tran_code_diff NOT IN UNNEST(xb.tran_code_exclusion_array) )
        AND ( cross_border_indicator_diff IN UNNEST(xb.xb_ind_inclusion_array)
          OR ARRAY_LENGTH(xb.xb_ind_inclusion_array) = 0 )
        AND ( cross_border_indicator_diff NOT IN UNNEST(xb.xb_ind_exclusion_array) )
        AND ( moto_ec_indicator_diff IN UNNEST(xb.moto_ind_inclusion_array)
          OR ARRAY_LENGTH(xb.moto_ind_inclusion_array) = 0 )
        AND ( moto_ec_indicator_diff NOT IN UNNEST(xb.moto_ind_exclusion_array) )
        AND ( cardholder_id_method_diff IN UNNEST(xb.chid_method_inclusion_array)
          OR ARRAY_LENGTH(xb.chid_method_inclusion_array) = 0 )
        AND ( cardholder_id_method_diff NOT IN UNNEST(xb.chid_method_exclusion_array))
        AND (file_date_diff >= coalesce(effective_date,
            '1900-01-01')
          AND file_date_diff < coalesce(end_date,
            '2099-12-31'))
      LEFT JOIN
        transformed_layer.dim_crd_typ dim_card_desc
      ON
        a.card_type_diff=dim_card_desc.card_type
        AND dim_card_desc.current_ind='0'
      LEFT JOIN
        transformed_layer.dim_chrg_typ dim_charge_desc
      ON
        a.corp_diff=dim_charge_desc.corporate
        AND a.charge_type_diff=dim_charge_desc.charge_type
        AND dim_charge_desc.current_ind='0'
      LEFT JOIN ( (
          SELECT
            * EXCEPT (r)
          FROM (
            SELECT
              dim.fee_program_ind,
              dim.ird,
              dim.card_type,
              dim.charge_type,
              dim.fee_program_desc,
              dim.interchange_rate_percent,
              dim.interchange_fee_amount,
              ROW_NUMBER() OVER (PARTITION BY card_type, charge_type ORDER BY ifnull(fee_program_ind, ird) DESC)r
            FROM
              transformed_layer.dim_intchg_prgm_cmplnc dim)
          WHERE
            r=1)) dimipc
      ON
        a.card_type_diff =dimipc.card_type
        AND a.charge_type_diff = dimipc.charge_type
      ) lcot
    LEFT JOIN (
      SELECT
        DISTINCT country_code_2a,
        mybank_geographical_region,
        mybank_region
      FROM
        transformed_layer.dim_iso_ctry_cd
      WHERE
        mybank_corporate = '014'
        AND current_ind = 0) dim_ctry
    ON
      lcot.region_diff =dim_ctry.mybank_region
    LEFT JOIN (
        SELECT
          DISTINCT mybank_settlement_currency_array,
    mybank_corporate,mybank_region
        FROM
          transformed_layer.dim_iso_ctry_cd
        WHERE
          mybank_corporate = '014'
        AND current_ind = 0) dim_ctry1
      ON
        dim_ctry1.mybank_corporate = lcot.corp_guid
        AND lcot.region_guid=dim_ctry1.mybank_region
    LEFT JOIN (
      SELECT
        DISTINCT lpad(CAST(mp_ica AS string),
          6,
          '0') mp_ica,
        vpaynet_bin,
        region,
        ROW_NUMBER() OVER(PARTITION BY region ORDER BY mp_ica DESC, vpaynet_bin DESC, region DESC) rn
      FROM
        transformed_layer.uk_mmbinsq_assc_parm
      WHERE
        corporate = '014'
        AND current_ind = '0') assc_parm1
    ON
      lcot.region_diff = assc_parm1.region
      AND rn=1)a)source
ON
  target.trans_sk_guid=source.trans_sk_guid
  AND target.type= source.type
  AND target.sub_type=source.sub_type
  AND target.fee_type=source.fee_type
  AND target.status=source.status
  AND target.exception_action_report_sk=source.exception_action_report_sk
  WHEN MATCHED THEN UPDATE SET target.etlbatchid=source.etlbatchid, target.update_date_time=source.update_date_time, target.trans_sk_guid=source.trans_sk_guid, target.chargeback_sk_chargeback_eod=source.chargeback_sk_chargeback_eod, target.member_balancing_sk=source.member_balancing_sk, target.mybank_vp_mp_reclass_items_sk=source.mybank_vp_mp_reclass_items_sk, target.exception_action_report_sk=source.exception_action_report_sk, target.type=source.type, target.sub_type=source.sub_type, target.fee_type=source.fee_type, target.geographical_region=source.geographical_region, target.mybank_acquirer_id=source.mybank_acquirer_id, target.hierarchy=source.hierarchy, target.merchant_number=source.merchant_number, target.merchant_number_int=source.merchant_number_int, target.acquirer_bin=source.acquirer_bin, target.order_id=CAST(source.order_id AS string), target.alpha_currency_code= CASE
    WHEN target.alpha_currency_code IS NULL
  OR LTRIM(target.alpha_currency_code,' ')='' THEN source.alpha_currency_code
  ELSE
  target.alpha_currency_code
END
  ,
  target.currency_exponent=
  CASE
    WHEN target.currency_exponent IS NULL OR TRIM(CAST(target.currency_exponent AS string),' ')='' THEN source.currency_exponent
  ELSE
  target.currency_exponent
END
  ,
  target.amount=
  CASE
    WHEN target.amount IS NULL OR TRIM(CAST(target.amount AS string),' ')='' THEN source.amount
  ELSE
  target.amount
END
  ,
  target.country_code=source.country_code,
  target.reference_transaction_id=CAST(source.reference_transaction_id AS string),
  target.status=source.status,
  target.reason_text=CAST(source.reason_text AS string),
  target.payment_method=source.payment_method,
  target.processing_date=source.processing_date,
  target.file_date=source.file_date,
  target.merchant_aggregate_reference=source.merchant_aggregate_reference,
  target.bin_aggregate_reference=source.bin_aggregate_reference,
  target.arn=
  CASE
    WHEN target.arn IS NULL OR TRIM(target.arn,' ')='' THEN source.arn
  ELSE
  target.arn
END
  ,
  target.settlement_alpha_currency_code=
  CASE
    WHEN target.settlement_alpha_currency_code IS NULL OR TRIM(target.settlement_alpha_currency_code,' ')='' THEN source.settlement_alpha_currency_code
  ELSE
  target.settlement_alpha_currency_code
END
  ,
  target.expected_settled_amount=
  CASE
    WHEN target.expected_settled_amount IS NULL OR TRIM(CAST(target.expected_settled_amount AS string),' ')='' THEN source.expected_settled_amount
  ELSE
  target.expected_settled_amount
END
  ,
  target.settlement_currency_exponent=
  CASE
    WHEN target.settlement_currency_exponent IS NULL OR TRIM(CAST(target.settlement_currency_exponent AS string),' ')='' THEN source.settlement_currency_exponent
  ELSE
  target.settlement_currency_exponent
END
  ,
  target.expected_settlement_date=source.expected_settlement_date,
  target.scheme_rate=source.scheme_rate,
  target.card_type=source.card_type,
  target.card_type_desc=source.card_type_desc,
  target.charge_type=source.charge_type,
  target.charge_type_desc=source.charge_type_desc,
  target.domestic_international_ind=source.domestic_international_ind,
  target.interchange_fee_program=source.interchange_fee_program,
  target.interchange_fee_program_desc=source.interchange_fee_program_desc,
  target.interchange_rate=source.interchange_rate,
  target.vp_mp_rejects_sk=source.vp_mp_rejects_sk,
  target.system_trace_audit_number=source.system_trace_audit_number,
  target.retrieval_ref_number=source.retrieval_ref_number,
  target.card_country_code=source.card_country_code,
  target.account_funding_source=source.account_funding_source,
  target.card_bin=source.card_bin,
  target.bin_detail_sk=source.bin_detail_sk
  WHEN NOT MATCHED
  THEN
INSERT
  ( mybank_itemized_detail_sk,
    etlbatchid,
    create_date_time,
    update_date_time,
    trans_sk_guid,
    chargeback_sk_chargeback_eod,
    member_balancing_sk,
    mybank_vp_mp_reclass_items_sk,
    vp_mp_rejects_sk,
    exception_action_report_sk,
    type,
    sub_type,
    fee_type,
    geographical_region,
    mybank_acquirer_id,
    transaction_id,
    hierarchy,
    merchant_number,
    merchant_number_int,
    acquirer_bin,
    order_id,
    alpha_currency_code,
    currency_exponent,
    amount,
    country_code,
    reference_transaction_id,
    status,
    reason_text,
    payment_method,
    processing_date,
    file_date,
    merchant_aggregate_reference,
    bin_aggregate_reference,
    arn,
    settlement_alpha_currency_code,
    expected_settled_amount,
    settlement_currency_exponent,
    expected_settlement_date,
    scheme_rate,
    card_type,
    card_type_desc,
    charge_type,
    charge_type_desc,
    merchant_sequence,
    domestic_international_ind,
    interchange_fee_program,
    interchange_fee_program_desc,
    interchange_rate,
    system_trace_audit_number,
    retrieval_ref_number,
	card_country_code,
	account_funding_source,
	card_bin,
	bin_detail_sk,
    event_timestamp )
VALUES
  ( mybank_itemized_detail_sk, etlbatchid, create_date_time, update_date_time, trans_sk_guid, chargeback_sk_chargeback_eod, member_balancing_sk, mybank_vp_mp_reclass_items_sk, vp_mp_rejects_sk, exception_action_report_sk, type, sub_type, fee_type, geographical_region, mybank_acquirer_id, transaction_id, hierarchy, merchant_number, merchant_number_int, acquirer_bin, CAST(order_id AS string), alpha_currency_code, currency_exponent, amount, country_code, CAST(reference_transaction_id AS string), status, CAST(reason_text AS string), payment_method, processing_date, file_date, merchant_aggregate_reference, bin_aggregate_reference, arn, settlement_alpha_currency_code, expected_settled_amount, settlement_currency_exponent, expected_settlement_date, scheme_rate, card_type, card_type_desc, charge_type,charge_type_desc, merchant_sequence, domestic_international_ind, interchange_fee_program, interchange_fee_program_desc, interchange_rate, system_trace_audit_number, retrieval_ref_number, card_country_code, account_funding_source, card_bin, bin_detail_sk, event_timestamp )"
  bq_run "$merge_itemized3" 'itemized-crossborder-uk'

#update transaction_id with transaction_id of transaction flow if any null found
bq query --use_legacy_sql=FALSE "UPDATE transformed_layer_commplat.mybank_itmz_dtl_uk main SET    main.transaction_id = temp.transaction_id FROM   (SELECT transaction_id,                trans_sk_guid,                type         FROM   transformed_layer_commplat.mybank_itmz_dtl_uk         WHERE  file_date >= date_sub(current_date(), INTERVAL 10 day) and type IN( 'SALE', 'REFUND', 'CASH_ADVANCE', 'UNKNOWN' ) and status in ('PROCESSED','FE_REJECTED') and exception_action_report_sk='-1') temp WHERE  file_date >= date_sub(current_date(), INTERVAL 10 day) and (main.transaction_id is null or trim(main.transaction_id)='')        AND main.trans_sk_guid = temp.trans_sk_guid        AND main.sub_type = 'TRANSACTION_FEE'        AND main.fee_type IN ( 'CROSSBORDER_FEE' )        AND main.status = 'PROCESSED' and main.exception_action_report_sk='-1'"

echo $global_var

