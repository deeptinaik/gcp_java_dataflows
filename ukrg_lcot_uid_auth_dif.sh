#!/bin/bash
#set -x

jbid=$1
jbid_arr=()
bq_run()
{
    jobID="lcotbqjob"`date '+%Y%m%d%H%M%S%3N' `
	jbid_arr+=($jobID)
    QUERY=$1 

printf "\n****************** Query To Execute: ${QUERY} ******************\n"
printf "\n****************** Command To Execute: bq query --use_legacy_sql=FALSE ${QUERY} ******************\n"

dry_run_output=`bq query --use_legacy_sql=FALSE --dry_run "${QUERY}"`

if [ $? -eq 0 ]
then
    printf "\n****************** $dry_run_output ******************\n"
    
    execution_output=`bq query --job_id=$jobID --use_legacy_sql=FALSE "${QUERY}"`
    
    if [ $? -eq 0 ]
    then
        printf "\n****************** Query Ran Successfully ******************\n"
        
        number_of_rows_affected=`bq show --format=prettyjson -j $jobID | grep "recordsWritten" | head -n 1 | cut -d':' -f2 |sed 's/[\", ]//g'`
        echo "Number Of Rows Affected: $number_of_rows_affected"
        
    else
        printf "\n****************** Query Execution Failed.... Casuse: $execution_output ******************\n"
        exit 2
    fi
else
    printf "\n****************** Query Validation Failed.... Casuse: $dry_run_output ******************\n"
    exit 1
fi
}

echo "******************Now Running script uk_lcot_guid_auth_dif.sh*************************"

lcot_query_1="create or replace table backup_xl_layer.uk_auth_dif_filter_dates_$jbid AS	--lcot_query_1 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh
(
SELECT $jbid as etlbatchid
	
	, cast( parse_date('%Y%m%d',substr(cast($jbid as string),1,8)) as date) as etlbatchid_date
	
	,  cast( rpad( format_date( '%Y%m%d',date_sub(max_dif_date,interval 10 day)),17,'0') as int64)  filter_date_etlbatchid_dif
    , cast( rpad( format_date( '%Y%m%d',date_sub(max_auth_date,interval 150 day)),17,'0') as int64) as filter_date_150_days_etlbatchid_auth
	, date_sub(max_auth_date,interval 150 day) filter_date_150_etlbatchid_date_auth
	, date_sub(max_auth_date,interval 5 day) filter_date_5_days_etlbatchid_date_auth
  , cast( rpad( format_date( '%Y%m%d',date_sub(max_auth_date,interval 5 day)),17,'0') as int64) as filter_date_5_days_etlbatchid_auth
	
	, date_sub(max_dif_date, interval 180 day) as filter_date_180_etlbatchid_date
  , cast( rpad( format_date( '%Y%m%d',date_sub(max_dif_date,interval 5 day)),17,'0') as int64)   etlbatchid_tran_full_join
  , date_sub(max_dif_date, interval 365 day) as filter_date_1_yr_etlbatchid_date_target
	, date_sub(CURRENT_DATE(), interval 20 day) as filter_date_20_etlbatchid_date_guid
	
	, date_sub(CURRENT_DATE(), interval 150 day) as filter_date_150_etlbatchid_date_guid
  from 
  (   select ifnull(parse_date('%Y%m%d' ,substr(dif_max,1,8) ), current_date) max_dif_date , 
  ifnull(parse_date('%Y%m%d' , substr(auth_max,1,8)), current_date)  max_auth_date 
  from 
 ( select cast( max(etlbatchid_dif) as string) dif_max , cast(max(etlbatchid_auth) as string) auth_max from xl_layer.lcot_uid_key_ukrg )
 )
	
);"

lcot_query_1_1="Create or replace table backup_xl_layer.uk_mpg_scorp as 
( 
	select distinct corporate
	from xl_layer.lcot_config_details 
	where application_name = 'eu_mpg_s' and current_ind = 0
)
"


lcot_query_2="declare filter_date_etlbatchid_dif int64;     --lcot_query_2 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh
set filter_date_etlbatchid_dif=(select filter_date_etlbatchid_dif from backup_xl_layer.uk_auth_dif_filter_dates_$jbid) ;


Create or replace table backup_xl_layer.temp_uk_dif_table_$jbid 
--cluster by  global_trid_diff,global_trid_source_diff
as 
(
with TRANS_BASE AS 
(
SELECT
    T.ukrg_trans_fact_SK AS trans_sk
   , LTRIM(T.merchant_number,'0') AS MERCHANT_NUMBER
   , T.transaction_amount AS TRANSACTION_AMOUNT
   , T.supplemental_authorization_amount AS SUP_AUTHORIZATION_AMOUNT
   , T.authorization_amount AS AUTHORIZATION_AMOUNT
   , REPLACE(ifnull(trim(UPPER(T.transaction_identifier)),''),' ','') AS TRANSACTION_ID
   , case when length(ltrim(REGEXP_REPLACE(trim(transaction_identifier), '[^a-zA-Z0-9]+', '') ,'0'))>4 
          then UPPER(substr(ltrim(REGEXP_REPLACE(trim(transaction_identifier), '[^a-zA-Z0-9]+', '') ,'0'),1,length(ltrim(REGEXP_REPLACE(trim(transaction_identifier), '[^a-zA-Z0-9]+', '') ,'0'))-1)) 
          else UPPER(ifnull(ltrim(REGEXP_REPLACE(trim(transaction_identifier), '[^a-zA-Z0-9]+', '') ,'0'),'')) 
      end AS TRANSACTION_ID_trim_last_char
   , T.transaction_identifier AS TRANSACTION_ID_ORIGINAL
   , ifnull( TRIM(UPPER(T.authorization_code)),'' ) AS AUTHORIZATION_CODE
   , T.transaction_date
   , T.authorization_date
   , cast(T.card_number_sk as string ) as card_number_sk_original_T
   -- ifnull(T.global_token,cast(T.card_number_sk as string))AS CARD_NUMBER_HK
   , cast(T.card_number_sk as string) AS CARD_NUMBER_HK
   , T.card_number_rk
   , REPLACE(trim(CAST(T.transaction_time AS STRING)),':','') AS transaction_time
   , trim(T.ref_number) as ref_number
   , T.corporate AS CORP
   , T.region
   , T.principal
   , T.associate
   , T.chain
   , T.merchant_name AS merchant_dba_name
   , T.original_transaction_ref_number AS ORIGINAL_TRANSACTION_REFERENCE
   , T.card_type
   , T.charge_type
   , T.transaction_code
   , T.all_source_terminal_id
   , T2.lcot_guid_key_sk AS lcot_guid_key_sk_tran
   , T.etlbatchid AS etlbatchid_tran
   , T.card_scheme AS card_scheme_diff
   , trim(T.global_trid)   AS global_trid_diff
   , trim(T.global_trid_source)  AS global_trid_source_diff

FROM 
 (
    SELECT
     ft.ukrg_trans_fact_SK,
	 ft.merchant_number,
	 ft.transaction_amount,
	 ft.supplemental_authorization_amount,
	 ft.authorization_amount,
	 ft.transaction_identifier,
	 ft.authorization_code,
     ft.transaction_date,
	 ft.authorization_date,
	 ft.card_number_sk,
	 ft.card_number_rk,
	 ft.transaction_time,
	 ft.ref_number,
	 ft.corporate, 
	 ft.region, 
	 ft.principal,
     ft.associate,
     ft.chain,
     ft.merchant_name,
     ft.original_transaction_ref_number ,
     ft.card_type,
     ft.charge_type,
     ft.transaction_code,
     ft.all_source_terminal_id,
     ft.etlbatchid ,
     ft.card_scheme,
     ft.global_trid,
     ft.global_trid_source
	 FROM
      xl_layer.vw_ukrg_tran_fact ft  join backup_xl_layer.uk_mpg_scorp uk on uk.corporate!=ft.corporate
    WHERE
       ft.etlbatchid > filter_date_etlbatchid_dif  and (ft.corporate not in ('051','014') and (ft.corporate not in ('052') or ft.region <> '05'))
	qualify row_number() over( partition by ukrg_trans_fact_sk order by etlbatchid DESC ) = 1
  ) T
  
LEFT JOIN 
  (
    SELECT
      T1.TRANS_SK
	  , T1.lcot_guid_key_sk
	  , MAX(T1.AUTH_SK) AS AUTH_SK
    FROM
       xl_layer.lcot_uid_key_ukrg T1
    WHERE
       T1.TRANS_SK <> '-1' 
	   and etlbatchid_dif >= filter_date_etlbatchid_dif
    GROUP BY
       T1.TRANS_SK
	   , lcot_guid_key_sk
  ) T2
      ON
        T.ukrg_trans_fact_SK = T2.TRANS_SK
      WHERE
        T2.TRANS_SK IS NULL
        OR T2.AUTH_SK = '-1' 
)
                
, TRANS AS 
(
SELECT
    TRANS_SK
    , T.MERCHANT_NUMBER AS MERCHANT_NUMBER_T
    , T.TRANSACTION_AMOUNT AS AMOUNT_T
    , T.SUP_AUTHORIZATION_AMOUNT AS AMOUNT_T1
    , T.AUTHORIZATION_AMOUNT AS AMOUNT_T2
    , T.TRANSACTION_ID AS TRAN_ID_T
    , T.CARD_TYPE as CARD_TYPE_T
	, substr(T.TRANSACTION_ID,1,4) AS TRAN_ID_1_4_T
	, TRANSACTION_ID_trim_last_char
	, SUBSTR(TRANSACTION_ID,LENGTH(TRANSACTION_ID)-3) as TRAN_ID_last_4_T
    , T.TRANSACTION_ID_ORIGINAL AS TRAN_ID_T_ORIGINAL
	, length(TRANSACTION_ID) as lenght_TRAN_ID_T
    , T.AUTHORIZATION_CODE AS AUTH_CODE_T
    , substr(substr(authorization_code, length(authorization_code)-3),1,4) as AUTH_CODE_last_4_T
	, SUBSTR(T.AUTHORIZATION_CODE,1,1) AS AUTH_CODE_first_letter_T
    , T.TRANSACTION_DATE AS TRAN_DATE_T
	, T.authorization_date AS AUTH_DATE_T
	, T.card_number_sk_original_T
    , T.CARD_NUMBER_HK AS CARD_NBR_T
    , T.CARD_NUMBER_RK AS CARD_NUMBER_RK_T
    , T.TRANSACTION_TIME AS TRAN_TIME_T
	, substr(T.TRANSACTION_TIME,1,4) AS TRAN_TIME_1_4_T
	, DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) as current_date_2_days
	, DATE_SUB(CURRENT_DATE(),INTERVAL 3 DAY) as current_date_3_days
	, DATE_SUB(CURRENT_DATE(),INTERVAL 4 DAY) as current_date_4_days
	, DATE_SUB(CURRENT_DATE(),INTERVAL 5 DAY) as current_date_5_days
	, DATE_ADD(TRANSACTION_DATE,INTERVAL 5 DAY) as TRAN_DATE_after_5_days_T
	, ORIGINAL_TRANSACTION_REFERENCE
    , SUBSTR(ORIGINAL_TRANSACTION_REFERENCE,1,2) as ORIGINAL_TRANSACTION_REFERENCE_1_2_T
	, SUBSTR(ORIGINAL_TRANSACTION_REFERENCE,1,4) as ORIGINAL_TRANSACTION_REFERENCE_1_4_T
    , T.ref_number AS REF_NUMBER_T
    , T.CORP AS CORP_T
    , T.REGION AS REGION_T
    , T.PRINCIPAL AS PRINCIPAL_T
    , T.ASSOCIATE AS ASSOCIATE_T
    , T.CHAIN AS CHAIN_T
    , T.MERCHANT_DBA_NAME AS MERCHANT_DBA_NAME_T
	, T.charge_type as CHARGE_TYPE_T
	, ifnull(T.all_source_terminal_id,'') as TERMINAL_ID_T
	, T.transaction_code as TRANSACTION_CODE_T  
    , T.lcot_guid_key_sk_tran
    , T.etlbatchid_tran
	, T.card_scheme_diff
	, T.global_trid_diff
	, T.global_trid_source_diff
    , SUM(T.TRANSACTION_AMOUNT) OVER (PARTITION BY T.MERCHANT_NUMBER, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK, TRIM(T.AUTHORIZATION_CODE), TRIM(T.TRANSACTION_ID)) AS SUM_AMOUNT_T
    , SUM(T.TRANSACTION_AMOUNT) OVER (PARTITION BY T.MERCHANT_NUMBER, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK, TRIM(T.AUTHORIZATION_CODE), TRIM(T.TRANSACTION_ID)) AS SUM_AMOUNT_T1
    , SUM(T.TRANSACTION_AMOUNT) OVER (PARTITION BY T.MERCHANT_NUMBER, T.CARD_NUMBER_HK, TRIM(T.AUTHORIZATION_CODE), TRIM(T.TRANSACTION_ID)) AS SUM_AMOUNT_T2
        
	, ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP11
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP21
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP31
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP41
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_DATE, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP51
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP61
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP71
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP81
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_DATE  desc, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP91
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP101
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP201
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, SUBSTR(T.TRANSACTION_TIME, 1, 4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP301
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP401
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP501
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), SUBSTR(TRIM(T.TRANSACTION_ID),1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP601
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.CARD_NUMBER_HK, T.AUTHORIZATION_CODE ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP701
		
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP12 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP22 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP32
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY  ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP42
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_DATE, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP52 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY  ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP62
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP72
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP82 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP92 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP102 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP202
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP302
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP402
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP502 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), SUBSTR(TRIM(T.TRANSACTION_ID),1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP602 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), T.CARD_NUMBER_HK, T.AUTHORIZATION_CODE ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP702
		
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP13
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP23
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP33
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE,SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK ) AS TRNP43
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_DATE, T.TRANSACTION_TIME,  ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP53
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE,T.TRANSACTION_TIME,T.CARD_NUMBER_HK ORDER BY  ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP63
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP73
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP83
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP93
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP103
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_TIME, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP203
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, SUBSTR(T.TRANSACTION_TIME,1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP303
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP403
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.TRANSACTION_ID, T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP503
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), SUBSTR(TRIM(T.TRANSACTION_ID),1,4), T.CARD_NUMBER_HK ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP603
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.AUTHORIZATION_AMOUNT AS string), T.CARD_NUMBER_HK, T.AUTHORIZATION_CODE ORDER BY ORIGINAL_TRANSACTION_REFERENCE, TRANS_SK DESC ) AS TRNP703
		
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, T.TRANSACTION_AMOUNT, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), CAST(T.AUTHORIZATION_AMOUNT AS string), TRANS_SK DESC ) AS TRNP4
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, T.AUTHORIZATION_CODE, T.TRANSACTION_DATE, T.CARD_NUMBER_HK ORDER BY T.TRANSACTION_TIME, ORIGINAL_TRANSACTION_REFERENCE, T.TRANSACTION_AMOUNT, CAST(T.SUP_AUTHORIZATION_AMOUNT AS string), CAST(T.AUTHORIZATION_AMOUNT AS string),TRANS_SK DESC ) AS TRNP5
   
    ---row number logic for ACOMPOS 
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.REF_NUMBER, T.AUTHORIZATION_DATE, T.CARD_NUMBER_HK ORDER BY ETLBATCHID_TRAN DESC, TRANS_SK DESC ) AS TRNP6
    , ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.REF_NUMBER, T.CARD_NUMBER_HK ORDER BY ETLBATCHID_TRAN DESC, TRANS_SK DESC ) AS TRNP7
	, ROW_NUMBER() OVER(PARTITION BY T.MERCHANT_NUMBER, CAST(T.TRANSACTION_AMOUNT AS string), T.TRANSACTION_ID, T.AUTHORIZATION_CODE, T.AUTHORIZATION_DATE, T.CARD_NUMBER_HK ORDER BY ETLBATCHID_TRAN DESC, TRANS_SK DESC ) AS TRNP8

   FROM
      TRANS_BASE T 
)
    select * from TRANS
);"

lcot_query_3="declare filter_date_5_days_etlbatchid_auth,filter_date_150_days_etlbatchid_auth int64;    --lcot_query_3 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh
declare filter_date_150_etlbatchid_date_auth,filter_date_5_days_etlbatchid_date_auth date;
set filter_date_5_days_etlbatchid_auth=(select filter_date_5_days_etlbatchid_auth from backup_xl_layer.uk_auth_dif_filter_dates_$jbid) ;
set filter_date_150_etlbatchid_date_auth = (select filter_date_150_etlbatchid_date_auth from backup_xl_layer.uk_auth_dif_filter_dates_$jbid) ;
set filter_date_150_days_etlbatchid_auth=(select filter_date_150_days_etlbatchid_auth from backup_xl_layer.uk_auth_dif_filter_dates_$jbid);
set filter_date_5_days_etlbatchid_date_auth=(select filter_date_5_days_etlbatchid_date_auth from backup_xl_layer.uk_auth_dif_filter_dates_$jbid);


Create or replace table backup_xl_layer.temp_uk_auth_table_$jbid as 
(
with AUTH_BASE AS 
(
SELECT distinct
     north_uk_authorization_fin_ft_sk AS NORTH_UK_AUTH_FIN_SK
    , LTRIM(AA.merchant_number,'0') AS merchant_number
    , AA.amount_1x
    , REPLACE(ifnull(trim(UPPER(network_reference_number)),''),' ','') AS NETWORK_REFERENCE_NUMBER
    , network_reference_number AS TRANSACTION_ID_ORIGINAL
    , ifnull(TRIM(UPPER(AA.approval_code)),'') AS APPROVAL_CODE
    , AA.tran_date
	, AA.card_number_sk as card_number_sk_original_A
    --, ifnull(AA.global_token,AA.card_number_sk) AS CARD_NUMBER_HK
	, AA.card_number_sk AS CARD_NUMBER_HK
    , AA.card_number_rk
    , REPLACE(trim(CAST(AA.tran_time AS STRING)),':','') AS tran_time
    , AA.terminal_id
    , AA.sequence_number
	, cast(response_code as int64) as resp_code_num_gnap_auth
	, settle_date_month
	, record_type as record_type_gnap_auth
	, rec_type as rec_type_gnap_auth
	, excep_reason_code as excep_reason_code_gnap_auth
    , retrieval_ref_number as retrieval_ref_number_auth
    , AA.lcot_GUID_KEY_SK AS lcot_guid_key_sk_auth
    , H.corporate AS CORP
    , H.region
    , H.principal
    , H.associate
    , H.chain
    , H.dba_name AS MERCHANT_NAME
    , AA.etlbatchid AS etlbatchid_auth
	, trim(global_trid) AS global_trid_auth
	, trim(global_trid_source)  AS global_trid_source_auth
	,global_trid_target
FROM 
 (
	select 
	AA.north_uk_authorization_fin_ft_sk
    ,LTRIM(AA.merchant_number,'0') AS merchant_number
	,AA.amount_1x
    ,AA.network_reference_number
    ,AA.approval_code
    ,AA.tran_date
    ,AA.global_token
    ,AA.card_number_sk
    ,AA.card_number_rk
    ,AA.tran_time
    ,AA.terminal_id
    ,AA.sequence_number
    ,AA.response_code
    ,AA.settle_date_month
    ,AA.record_type
    ,AA.rec_type
    ,AA.excep_reason_code
    ,AA.retrieval_ref_number
    ,AA.etlbatchid
    ,AA.global_trid
    ,AA.global_trid_source
    , AUTH.lcot_GUID_KEY_SK
	,global_trid_target
	from 
		( 
			select 
	   north_uk_authorization_fin_ft_sk
      ,merchant_number
      ,amount_1x
      ,network_reference_number
      ,approval_code
      ,tran_date
      ,card_number_sk
      ,global_token
      ,card_number_rk
      ,tran_time
      ,terminal_id
      ,sequence_number
      ,response_code
      ,settle_date_month
      ,record_type
      ,rec_type
      ,excep_reason_code
      ,retrieval_ref_number
      ,etlbatchid
	  ,global_trid as global_trid_target
      ,IFNULL(global_trid, (TO_BASE64(SHA512(CONCAT(IFNULL(amount_1x,0),TRIM(SUBSTRING(IFNULL(TRIM(network_reference_number),''),1,15)),
	  LTRIM(TRIM(merchant_number),'0'),tran_date))))) AS global_trid
      ,IFNULL(global_trid_source,'') AS global_trid_source
       from xl_layer.vw_north_uk_authorization_fin_ft 
			where etl_batch_date >= filter_date_150_etlbatchid_date_auth
		qualify row_number() over( partition by north_uk_authorization_fin_ft_sk order by etlbatchid DESC ) = 1
			
		) AA
		
	join 
	(
		select 
			GUID_BASE.AUTH_SK
			, max(GUID_BASE.lcot_GUID_KEY_SK) AS lcot_GUID_KEY_SK
		from 
		(
			select 
				GUID_AUTH_DIF.AUTH_SK, lcot_GUID_KEY_SK 
			FROM 
			(
				SELECT guid_auth.AUTH_SK , guid_auth.lcot_GUID_KEY_SK
				from 
				(
					select 
						AUTH_SK
						, MERCHANT_NUMBER
						, CARD_NUMBER_SK 
						, max(case when TRANS_SK = '-1' then lcot_GUID_KEY_SK else cast(NULL as string) end) as lcot_GUID_KEY_SK
					from xl_layer.lcot_uid_key_ukrg 
					where 
						AUTH_SK <> '-1'
						and etlbatchid_auth >= filter_date_150_days_etlbatchid_auth
					
					group by 
						AUTH_SK, MERCHANT_NUMBER, CARD_NUMBER_SK 
				) guid_auth
				
				join 
				(	
					select 
						MERCHANT_NUMBER_T, CARD_NBR_T 
					from 
						backup_xl_layer.temp_uk_dif_table_$jbid
					group by MERCHANT_NUMBER_T, CARD_NBR_T
				) dif
			
				on guid_auth.MERCHANT_NUMBER = dif.MERCHANT_NUMBER_T
				and guid_auth.CARD_NUMBER_SK = dif.CARD_NBR_T
		
			) GUID_AUTH_DIF
  
			union all 
			(
				select BASE.AUTH_SK, BASE.lcot_GUID_KEY_SK 
				from 
				(
					select 
						north_uk_authorization_fin_ft_sk AS AUTH_SK 
						, cast(NULL as string) as lcot_GUID_KEY_SK  
					from xl_layer.vw_north_uk_authorization_fin_ft 
					where etl_batch_date >= filter_date_5_days_etlbatchid_date_auth
					qualify row_number() over( partition by north_uk_authorization_fin_ft_sk order by etlbatchid DESC ) = 1
					
				) BASE
			
				left join 
				(
					select AUTH_SK from xl_layer.lcot_uid_key_ukrg 
					where 
						AUTH_SK <> '-1' 
						and etlbatchid_auth >= filter_date_5_days_etlbatchid_auth
					group by AUTH_SK
				) GUID
		
				on BASE.AUTH_SK = GUID.AUTH_SK
				WHERE 
					GUID.AUTH_SK IS NULL	
			)
		) GUID_BASE
		group by AUTH_SK
	) AUTH 

	on AA.north_uk_authorization_fin_ft_sk = AUTH.AUTH_SK

 ) AA
  
  LEFT OUTER JOIN 
  (
        SELECT
           corporate, region, principal, associate, chain, dba_name, LTRIM(merchant_number,'0') as merchant_number
        FROM
           consumption_layer.dim_merchant_information
        WHERE
           CURRENT_IND='0'
   ) H
  
  ON AA.merchant_number = H.merchant_number

)
                
, AUTH AS
(
SELECT
    NORTH_UK_AUTH_FIN_SK
   , A.MERCHANT_NUMBER AS MERCHANT_NUMBER_A
   , (A.AMOUNT_1X)/100 AS AMOUNT_A
   , (A.AMOUNT_1X) AS AMOUNT_A1
   , NETWORK_REFERENCE_NUMBER AS TRAN_ID_A
   , SUBSTR(NETWORK_REFERENCE_NUMBER,1,4) AS TRAN_ID_1_4_A
   , SUBSTR(SUBSTR(NETWORK_REFERENCE_NUMBER,LENGTH(NETWORK_REFERENCE_NUMBER)-5),1,4) AS TRAN_ID_last_6_1_4_A
   , SUBSTR(NETWORK_REFERENCE_NUMBER,LENGTH(NETWORK_REFERENCE_NUMBER)-3) as TRAN_ID_last_4_A
   , SUBSTR(NETWORK_REFERENCE_NUMBER,1,1) as TRAN_ID_first_letter_A
   , TRANSACTION_ID_ORIGINAL AS TRAN_ID_A_ORIGINAL
   , A.APPROVAL_CODE AS AUTH_CODE_A
   , substr(substr(approval_code, length(approval_code)-3),1,4) as AUTH_CODE_last_4_A
   , substr(substr(approval_code, length(approval_code)-1),1,2) as AUTH_CODE_last_2_A
   , A.TRAN_DATE AS TRAN_DATE_A
   , A.card_number_sk_original_A
   , A.CARD_NUMBER_HK AS CARD_NBR_A
   , A.CARD_NUMBER_RK AS CARD_NUMBER_RK_A
   , A.TRAN_TIME AS TRAN_TIME_A
   , substr(A.TRAN_TIME,1,4) AS TRAN_TIME_1_4_A
   , A.resp_code_num_gnap_auth
   , A.settle_date_month as settle_date_month_A
   , A.record_type_gnap_auth
   , A.rec_type_gnap_auth
   , A.excep_reason_code_gnap_auth
   , A.retrieval_ref_number_auth as retrieval_ref_number_a
   , DATE_ADD(TRAN_DATE,INTERVAL 8 DAY) as TRAN_DATE_after_8_days_A
   , DATE_SUB(TRAN_DATE,INTERVAL 10 DAY) as TRAN_DATE_before_10_days_A
   , DATE_ADD(TRAN_DATE,INTERVAL 10 DAY) as TRAN_DATE_after_10_days_A
   , DATE_ADD(TRAN_DATE,INTERVAL 30 DAY) as TRAN_DATE_after_30_days_A
   , A.CORP AS CORP_A
   , A.REGION AS REGION_A
   , A.PRINCIPAL AS PRINCIPAL_A
   , A.ASSOCIATE AS ASSOCIATE_A
   , A.CHAIN AS CHAIN_A
   , A.MERCHANT_NAME AS MERCHANT_DBA_NAME_A
   , ifnull(A.terminal_id, '') AS TERMINAL_ID_A
   , A.lcot_guid_key_sk_auth
   , A.etlbatchid_auth
   , A.etlbatchid_auth AS etlbatchid_auth_main
   , A.global_trid_auth
   , A.global_trid_source_auth
   ,global_trid_target
		
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A. NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.TRAN_DATE, SUBSTR(A.TRAN_TIME,1,6), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP1
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP2
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP3
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, A.AMOUNT_1X, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP4
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-3), A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP5
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.CARD_NUMBER_HK ORDER BY A.TRAN_DATE, A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP6
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.APPROVAL_CODE, A.TRAN_DATE, SUBSTR(A.TRAN_TIME,1,4), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP7
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP8
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_DATE desc, A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP9
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-5),1,4), A.APPROVAL_CODE, A.TRAN_DATE, SUBSTR(A.TRAN_TIME,1,6), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP10
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.TRAN_DATE, SUBSTR(A.TRAN_TIME,1,4), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP11
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-5),1,4), A.APPROVAL_CODE, A.TRAN_DATE, SUBSTR(A.TRAN_TIME,1,4), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP12
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, SUBSTR(SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-5),1,4), A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP13
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP14
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-3), A.APPROVAL_CODE, A.TRAN_DATE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP15
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, A.CARD_NUMBER_HK ORDER BY A.APPROVAL_CODE, A.TRAN_DATE, A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP16
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, SUBSTR(A.TRAN_TIME,1,6), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP17
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.APPROVAL_CODE, SUBSTR(A.TRAN_TIME,1,4), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP18
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),LENGTH(TRIM(A.NETWORK_REFERENCE_NUMBER))-5),1,4), A.APPROVAL_CODE, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP19
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.CARD_NUMBER_HK ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP20
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), SUBSTR(TRIM(A.NETWORK_REFERENCE_NUMBER),1,4), A.CARD_NUMBER_HK ORDER BY TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP21
   , ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.CARD_NUMBER_HK, A.APPROVAL_CODE ORDER BY A.TRAN_TIME, TERMINAL_ID, SEQUENCE_NUMBER, CAST(A.AMOUNT_1X AS string), NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP22

    --- row number logic for ACOMPOS
   ,ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.TRAN_DATE, A.retrieval_ref_number_auth, A.APPROVAL_CODE , A.CARD_NUMBER_HK ORDER BY ETLBATCHID_AUTH DESC, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP23
   ,ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.retrieval_ref_number_auth, A.APPROVAL_CODE , A.CARD_NUMBER_HK ORDER BY ETLBATCHID_AUTH DESC, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP24
   ,ROW_NUMBER() OVER(PARTITION BY A.MERCHANT_NUMBER, CAST(A.AMOUNT_1X AS string), A.NETWORK_REFERENCE_NUMBER, A.TRAN_DATE, A.APPROVAL_CODE , A.CARD_NUMBER_HK ORDER BY ETLBATCHID_AUTH DESC, NORTH_UK_AUTH_FIN_SK DESC ) AS ARNP25
   
FROM
   AUTH_BASE A 
)
select * from AUTH 
);"


lcot_query_4="Create or replace table backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_ST1_$jbid AS     --lcot_query_4 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh
(
with AUTH_DIF_JOIN_IND_ALL_1 AS
( SELECT 
    *
    , CAST(0.1 as NUMERIC) AS JOININD
	    , '20241020' AS JOININDDATE
FROM 
    
	( select * from backup_xl_layer.temp_uk_auth_table_$jbid
	   where TRIM(ifnull(global_trid_auth,''))<>'' and 
	(
	    ifnull(resp_code_num_gnap_auth,0) < 50 
	    AND ifnull(record_type_gnap_auth,'') <> 'mpg_s'
        AND ( ifnull(rec_type_gnap_auth,'') <> '21' OR ifnull(excep_reason_code_gnap_auth,'') < '500' OR ifnull(excep_reason_code_gnap_auth,'') > '511' )
	)
	OR
	(
	    ifnull(record_type_gnap_auth,'') = 'mpg_s'
	    AND ifnull(resp_code_num_gnap_auth,0) <> 05 
	
	)
	)A
	
JOIN    
      ( select * from backup_xl_layer.temp_uk_dif_table_$jbid where card_scheme_diff <> 'OB'
	      and TRIM(ifnull(global_trid_diff,''))<>'' 
	  ) T
	  
	ON  A.global_trid_auth = T.global_trid_diff 
		and A.global_trid_source_auth= T.global_trid_source_diff

)   
, AUTH_DIF_JOIN_IND_ALL_2 AS
(
SELECT
   *
  , CASE
      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP1 = TRNP11) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') ) 
	  THEN '1_20241020'
	  
      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND(AMOUNT_T1 = AMOUNT_A1 AND ARNP1 = TRNP12)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_T = TRAN_TIME_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') ) 
	  THEN '2_20241020'
      
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (ARNP1 = TRNP13 AND AMOUNT_T2 = AMOUNT_A) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '3_20241020'

    when
    		MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP1 = TRNP11) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '4_20241020'
	  
      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND(AMOUNT_T1 = AMOUNT_A1 AND ARNP1 = TRNP12)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_T = TRAN_TIME_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '5_20241020'
      
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (ARNP1 = TRNP13 AND AMOUNT_T2 = AMOUNT_A) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '6_20241020'
    
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T = AMOUNT_A AND ARNP11 = TRNP41)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '7_20241020'
	  
      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T1 = AMOUNT_A1 AND ARNP11 = TRNP42) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '8_20241020'
      
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (ARNP11 = TRNP43 AND AMOUNT_T2 = AMOUNT_A)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '9_20241020'

	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP11 = TRNP41) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '10_20241020'

	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (round(AMOUNT_T) = round(AMOUNT_A) AND ARNP11 = TRNP41) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '11_20241020'
    
     WHEN
	  		MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP23 = TRNP6 )
            AND AUTH_DATE_T = TRAN_DATE_A
            AND REF_NUMBER_T = retrieval_ref_number_a
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND CARD_TYPE_T='37' AND record_type_gnap_auth = 'ACOMPOS'
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
    THEN '11.1_20241020'
    
    WHEN
	  		MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP24 = TRNP7 )
         -- AND AUTH_DATE_T = TRAN_DATE_A
            AND REF_NUMBER_T = retrieval_ref_number_a
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND CARD_TYPE_T='37' AND record_type_gnap_auth = 'ACOMPOS'
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
    THEN '11.2_20241020'
    
    WHEN
	  		MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP25 = TRNP8 )
            AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND CARD_TYPE_T='37' AND record_type_gnap_auth = 'ACOMPOS'
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
    THEN '11.3_20241020'
    
    WHEN
	  		MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP21 = TRNP601 and ARNP6 = TRNP51)    
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND CARD_TYPE_T='37'
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
    THEN '11.4_20241020'

	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T) = (AMOUNT_A) AND ARNP11 = TRNP41) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '12_20241020' 
    
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP10 = TRNP11) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '13_20241020'
      
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND(AMOUNT_T1 = AMOUNT_A1	AND ARNP10 = TRNP12) 
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_T = TRAN_TIME_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '14_20241020'
      
	  WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (ARNP10 = TRNP13 AND AMOUNT_T2 = AMOUNT_A) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	  THEN '15_20241020'

      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T = AMOUNT_A AND ARNP12 = TRNP41)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '16_20241020'
	
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND(AMOUNT_T1 = AMOUNT_A1 AND ARNP12 = TRNP42) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '17_20241020'
     
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (ARNP12 = TRNP43 AND AMOUNT_T2 = AMOUNT_A)
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '18_20241020'
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP2 = TRNP21) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP2 = TRNP22) OR (AMOUNT_T2 = AMOUNT_A AND ARNP2 = TRNP23)) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '19_20241020'
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP2 = TRNP21) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP2 = TRNP22) OR (AMOUNT_T2 = AMOUNT_A AND ARNP2 = TRNP23)) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '20_20241020'
    
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP15 = TRNP21) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP15 = TRNP22) OR (ARNP15 = TRNP23 AND AMOUNT_T2 = AMOUNT_A))
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') ) 
	 THEN '21_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP7 = TRNP61) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP7 = TRNP62) OR (ARNP7 = TRNP63 AND AMOUNT_T2 = AMOUNT_A)) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_T = TRAN_TIME_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '22_20241020'

     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP7 = TRNP71) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP7 = TRNP72) OR (ARNP7 = TRNP73 AND AMOUNT_T2 = AMOUNT_A))
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '23_20241020'

     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '23.1_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP16 = TRNP101) 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND AUTH_DATE_T >= TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '24_20241020'

     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T1 = AMOUNT_A1 AND ARNP16 = TRNP102)
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A
			AND AUTH_DATE_T >= TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '25_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T2 = AMOUNT_A AND ARNP16 = TRNP103) 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A
			AND AUTH_DATE_T >= TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '26_20241020'
	 
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP21 = TRNP601) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP21 = TRNP602) OR (AMOUNT_T2 = AMOUNT_A AND ARNP21 = TRNP603))
			AND TRAN_DATE_T BETWEEN TRAN_DATE_before_10_days_A AND TRAN_DATE_after_10_days_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A  
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '27_20241020'

   	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A) OR (AMOUNT_T1 = AMOUNT_A1) OR (AMOUNT_T2 = AMOUNT_A ))
			AND TRAN_DATE_T BETWEEN TRAN_DATE_before_10_days_A AND TRAN_DATE_after_10_days_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A  
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '28_20241020'
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP21 = TRNP601) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP21 = TRNP602) OR (AMOUNT_T2 = AMOUNT_A AND ARNP21 = TRNP603))
			AND TRAN_DATE_T BETWEEN TRAN_DATE_before_10_days_A AND TRAN_DATE_after_10_days_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND TRAN_DATE_T <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '29_20241020'
	 
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP3 = TRNP31) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP3 = TRNP32) OR (AMOUNT_T2 = AMOUNT_A AND ARNP3 = TRNP33)) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '30_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP17 = TRNP201) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP17 = TRNP202) OR (ARNP17 = TRNP203 AND AMOUNT_T2 = AMOUNT_A))
			AND TRAN_TIME_T = TRAN_TIME_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '31_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP18 = TRNP301) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP18 = TRNP302) OR (ARNP18 = TRNP303 AND AMOUNT_T2 = AMOUNT_A)) 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '32_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP19 = TRNP401) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP19 = TRNP402) OR (ARNP19 = TRNP403 AND AMOUNT_T2 = AMOUNT_A))
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') ) 
	 THEN '33_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP8 = TRNP81) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP8 = TRNP82) OR (AMOUNT_T2 = AMOUNT_A AND ARNP8 = TRNP83)) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '34_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND ((AMOUNT_T = AMOUNT_A AND ARNP20 = TRNP501) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP20 = TRNP502) OR (AMOUNT_T2 = AMOUNT_A AND ARNP20 = TRNP503))
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_T = TRAN_ID_A
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND AUTH_DATE_T >= TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '35_20241020'

    
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T = AMOUNT_A AND ARNP22 = TRNP701) 
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '39_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T1 = AMOUNT_A1 AND ARNP22 = TRNP702) 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A='' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '40_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T2 = AMOUNT_A AND ARNP22 = TRNP703)
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '41_20241020'
	 
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND ((AMOUNT_T = AMOUNT_A AND ARNP6 = TRNP51) OR (AMOUNT_T1 = AMOUNT_A1 AND ARNP6 = TRNP52) OR (AMOUNT_T2 = AMOUNT_A AND ARNP6 = TRNP53) ) 
			AND (TRAN_DATE_T BETWEEN TRAN_DATE_before_10_days_A AND TRAN_DATE_after_10_days_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '42_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A
			AND TRNP4 = ARNP4 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '43_20241020'
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND TRNP4 = ARNP13 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '44_20241020'
     
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' )
			AND TRNP5 = ARNP14 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '45_20241020'
	 
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (SUM_AMOUNT_T = AMOUNT_A) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND AUTH_DATE_T = TRAN_DATE_A
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '46_20241020'
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (SUM_AMOUNT_T = AMOUNT_A) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '47_20241020'   
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (SUM_AMOUNT_T1 = AMOUNT_A)
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '48_20241020'
     
     
     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (SUM_AMOUNT_T2 = AMOUNT_A) 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND AUTH_DATE_T = TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '49_20241020'

     WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (SUM_AMOUNT_T2 = AMOUNT_A) 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '50_20241020'
   
      WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (SUM_AMOUNT_T2 = AMOUNT_A)
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '51_20241020'

	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND AUTH_DATE_T = TRAN_DATE_A
			AND TRANSACTION_ID_trim_last_char = TRAN_ID_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '52_20241020'

   	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
     	    AND TRANSACTION_ID_trim_last_char = TRAN_ID_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )			
	 THEN '53_20241020'
   
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND TRAN_DATE_T = TRAN_DATE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '54_20241020'

	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A	
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )		
     THEN '55_20241020'

	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND AUTH_CODE_T = AUTH_CODE_A 
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '56_20241020'
	 
	 WHEN 													
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND (AUTH_CODE_T = '' OR  AUTH_CODE_A = '')
			AND TRAN_ID_T = TRAN_ID_last_6_1_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '57_20241020'
	 
	 WHEN 																	
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
			AND TRAN_ID_T = TRAN_ID_last_4_A 
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '58_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_last_4_T = AUTH_CODE_last_4_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '62_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T = AUTH_CODE_last_2_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '63_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_T = TRAN_ID_A 
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '64_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_T = TRAN_ID_A 
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' AND AUTH_CODE_T <> AUTH_CODE_A)
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '65_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_T = TRAN_ID_last_4_A
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '66_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_T = TRAN_ID_last_4_A
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '67_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND TRAN_ID_last_4_T = TRAN_ID_last_4_A
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '68_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND TRAN_ID_last_4_T = TRAN_ID_last_4_A
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '69_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_A = ''
			AND lenght_TRAN_ID_T = 4
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '70_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_A = ''
			AND lenght_TRAN_ID_T = 4
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '71_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND AMOUNT_T = AMOUNT_A 
			AND (AUTH_CODE_T <> '' AND AUTH_CODE_A <> '')
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '72_20241020'
     

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_T = TRAN_ID_last_4_A
			AND (AUTH_CODE_T = '' AND AUTH_CODE_A <> '')
			AND TRAN_ID_T <> ''
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '73_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_T = settle_date_month_A
			AND (AUTH_CODE_T = '' AND AUTH_CODE_A <> '')
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '74_20241020'
     

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_CODE_T = AUTH_CODE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '76_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ORIGINAL_TRANSACTION_REFERENCE_1_2_T = 'e_'
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '77_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ORIGINAL_TRANSACTION_REFERENCE_1_4_T = 'und_'
			AND AUTH_CODE_T <> AUTH_CODE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '78_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_CODE_T =''
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '79_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_CODE_T =''
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '80_20241020'

      WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '81_20241020'


     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A 
			AND TRAN_ID_first_letter_A = 'M'
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND lenght_TRAN_ID_T > 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '82_20241020'


     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '83_20241020'
	 
	 
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND TRAN_ID_T = TRAN_ID_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '84_20241020' 

     
    WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '84.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '84.3_20241020'
       
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '85_20241020'

	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_CODE_T =''
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '86_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '87_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T = AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '88_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '89_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'T'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '89.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '89.2_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '89.3_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND ( AUTH_CODE_T ='' AND AUTH_CODE_A <> '' )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '89.4_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = 'W'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_CODE_T =''
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '90_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = 'S'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '91_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = 'S'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '91.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = 'S'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '91.2_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_first_letter_A = '4'
			AND TRAN_ID_T = TRAN_ID_A
			AND lenght_TRAN_ID_T > 4
			AND AUTH_CODE_T = AUTH_CODE_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '91.3_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = '4'
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND lenght_TRAN_ID_T > 4
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '92_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = '4'
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND lenght_TRAN_ID_T > 4
			AND TRAN_DATE_T <= current_date_3_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '92.1_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'B'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '93_20241020'


     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'B'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( AUTH_CODE_T = AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '93.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'B'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '93.2_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'B'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND TRAN_DATE_T <= current_date_2_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '93.3_20241020'

      
	WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_T =''
			AND AUTH_CODE_T = '000000'
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_T =''
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRAN_ID_T =''
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94.2_20241020'

	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A AND ARNP9 = TRNP91) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '94.3_20241020'
 
 	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A ) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND TRAN_TIME_1_4_T = TRAN_TIME_1_4_A
			AND CARD_NBR_T = CARD_NBR_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' )
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '94.4_20241020'

    
 	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A 
			AND (AMOUNT_T = AMOUNT_A ) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' ) 
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '94.5_20241020'
 
     
	 WHEN 
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND (AMOUNT_T1 = AMOUNT_A1 AND ARNP9 = TRNP92) 
			AND TRAN_DATE_T = TRAN_DATE_A 
			AND CARD_NBR_T = CARD_NBR_A 
			AND ( TRAN_ID_T  = '' OR TRAN_ID_A = '' )
			AND ( AUTH_CODE_T = '' OR AUTH_CODE_A = '' ) 
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
	 THEN '94.6_20241020'

   
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND (
					( TRAN_ID_T = '' OR  TRAN_ID_A = ''  OR TRAN_ID_T = TRAN_ID_A )
					  AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
				)
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94.7_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND (
					( TRAN_ID_T = '' OR  TRAN_ID_A = ''  OR TRAN_ID_T = TRAN_ID_A )
					  AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
				)
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94.8_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND CARD_NBR_T = CARD_NBR_A
			AND (
					( TRAN_ID_T = '' OR  TRAN_ID_A = ''  OR TRAN_ID_T = TRAN_ID_A )
					  AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
				)
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '94.9_20241020'


     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND (
					( TRAN_ID_T = '' OR  TRAN_ID_A = ''  OR TRAN_ID_T = TRAN_ID_A )
					  AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
				)
			AND TRAN_DATE_T <= current_date_3_days
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_ID_T =''
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AUTH_DATE_T >= TRAN_DATE_A
			AND (
					( TRAN_ID_T = '' OR  TRAN_ID_A = ''  OR TRAN_ID_T = TRAN_ID_A )
					  AND ( AUTH_CODE_T = '' OR  AUTH_CODE_A = '' OR AUTH_CODE_T = AUTH_CODE_A)
				)
			AND TRAN_DATE_T <= current_date_5_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.2_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )
			AND AMOUNT_T <= AMOUNT_A			
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.3_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND AMOUNT_T <= AMOUNT_A
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_8_days_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.4_20241020'
	 
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND AMOUNT_T <= AMOUNT_A
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.5_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T = AUTH_CODE_A ) 
			AND lenght_TRAN_ID_T = 4
			AND AMOUNT_T <= AMOUNT_A
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.6_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRAN_TIME_T = '000000'
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )  
			AND TRAN_ID_T <> TRAN_ID_A 
			AND TRAN_ID_T <> '' AND TRAN_ID_A <> ''
			AND TRAN_DATE_T < TRAN_DATE_A
			AND TRAN_DATE_A BETWEEN TRAN_DATE_T AND TRAN_DATE_after_5_days_T
			AND TRAN_DATE_A <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.7_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )  
			AND TRAN_ID_T <> TRAN_ID_A 
			AND TRAN_ID_T <> '' AND TRAN_ID_A <> ''
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND TRAN_ID_last_4_T = TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.8_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND AUTH_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )  
			AND TRAN_ID_T <> TRAN_ID_A 
			AND TRAN_ID_T <> '' AND TRAN_ID_A <> ''
			AND TRAN_ID_1_4_T = TRAN_ID_1_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.81_20241020'

          WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND AMOUNT_T = AMOUNT_A
			AND TRANSACTION_CODE_T <> '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T = '' AND AUTH_CODE_A <> '' )  
			AND TRAN_ID_T <> TRAN_ID_A 
			AND TRAN_ID_T <> '' AND TRAN_ID_A <> ''
			AND TRAN_DATE_T < TRAN_DATE_A
			AND TRAN_DATE_A BETWEEN TRAN_DATE_T AND TRAN_DATE_after_5_days_T
			AND TRAN_DATE_A <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '95.9_20241020'


     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_8_days_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND AMOUNT_T = AMOUNT_A
			AND AUTH_CODE_first_letter_T = '0'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.1_20241020'
     
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND AUTH_CODE_first_letter_T = '0'
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.21_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND AUTH_CODE_first_letter_T = '0'
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.22_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND AMOUNT_T = AMOUNT_A
			AND AUTH_CODE_first_letter_T = '0'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.3_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND AUTH_CODE_first_letter_T = '0'
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.4_20241020'   

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND lenght_TRAN_ID_T = 4
			AND TRAN_DATE_T > TRAN_DATE_A
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '96.5_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND TRAN_ID_first_letter_A = '4'
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = ''
			AND TRAN_ID_first_letter_A = '4'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND TRAN_ID_first_letter_A = '4'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.2_20241020'

    WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.3_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_ID_first_letter_A = 'M'
			AND lenght_TRAN_ID_T = 4
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.4_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND TRAN_ID_first_letter_A <> '4'
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' and AUTH_CODE_T <> AUTH_CODE_A )
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.5_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND TRAN_ID_first_letter_A <> '4'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.6_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND TRAN_ID_first_letter_A <> '4'
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.7_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T is null
			AND TRAN_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.8_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T is null
			AND TRAN_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '97.9_20241020' 

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T = '' 
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AUTH_DATE_T is null
			AND TRAN_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T <> TRAN_ID_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T <> TRAN_ID_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.2_20241020'
	 
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T <> TRAN_ID_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.3_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T <> TRAN_ID_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' )  
			AND TRAN_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.4_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T = TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T <> TRAN_ID_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T <> AUTH_CODE_A AND AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' )  
			AND TRAN_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.5_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.6_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.61_20241020'
	 
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND TRAN_ID_T > TRAN_ID_last_4_A
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.7_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_DATE_T >= TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T BETWEEN TRAN_DATE_after_8_days_A AND TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.8_20241020'
	 
     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_DATE_T >= TRAN_DATE_A
			AND TRANSACTION_CODE_T = '06'
			AND lenght_TRAN_ID_T >= 4
			AND ( AUTH_CODE_T = '' AND AUTH_CODE_A <> '' ) 
			AND TRAN_DATE_T > TRAN_DATE_after_30_days_A
			AND TRAN_DATE_T <= current_date_2_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '98.9_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_DATE_T < TRAN_DATE_A
			AND AMOUNT_T <= AMOUNT_A
			AND TRANSACTION_CODE_T = '06'
			AND ( TRAN_ID_T = '' AND TRAN_ID_A <> '' )
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' AND AUTH_CODE_T <> AUTH_CODE_A )
			AND etlbatchid_tran >= etlbatchid_auth
			AND TRAN_DATE_A BETWEEN TRAN_DATE_T AND TRAN_DATE_after_5_days_T
			AND TRAN_DATE_A <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '99_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_DATE_T < TRAN_DATE_A
			AND AMOUNT_T <= AMOUNT_A
			AND TRANSACTION_CODE_T = '06'
			AND ( TRAN_ID_T = '' AND TRAN_ID_A <> '' )
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' AND AUTH_CODE_T <> AUTH_CODE_A )
			AND etlbatchid_tran <= etlbatchid_auth
			AND TRAN_DATE_A BETWEEN TRAN_DATE_T AND TRAN_DATE_after_5_days_T
			AND TRAN_DATE_A <= current_date_3_days
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '99.1_20241020'

     WHEN  
			MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
			AND CARD_NBR_T = CARD_NBR_A
			AND TRAN_DATE_T <> TRAN_DATE_A
			AND TRAN_DATE_T > TRAN_DATE_A
			AND AMOUNT_T = AMOUNT_A
			AND TRANSACTION_CODE_T = '06'
			AND ( lenght_TRAN_ID_T = 4 AND TRAN_ID_A <> '' )
			AND TRAN_ID_T < TRAN_ID_last_4_A
			AND TRAN_ID_T > settle_date_month_A
			AND ( AUTH_CODE_T <> '' AND AUTH_CODE_A <> '' AND AUTH_CODE_T <> AUTH_CODE_A )
			AND TRAN_DATE_T BETWEEN TRAN_DATE_A AND TRAN_DATE_after_8_days_A
			AND ( TERMINAL_ID_T = TERMINAL_ID_A OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) <> '') OR (TRIM(TERMINAL_ID_T) = '' AND TRIM(TERMINAL_ID_A) = '') )
     THEN '99.2_20241020'

    ELSE '100_20241020'

  END AS JOININD_JOININDDATE

FROM
    (
	select * 
	from backup_xl_layer.temp_uk_auth_table_$jbid 
	where
	(
	    ifnull(resp_code_num_gnap_auth,0) < 50 
	    AND ifnull(record_type_gnap_auth,'') <> 'mpg_s'
        AND ( ifnull(rec_type_gnap_auth,'') <> '21' OR ifnull(excep_reason_code_gnap_auth,'') < '500' OR ifnull(excep_reason_code_gnap_auth,'') > '511' )
	)
	OR
	(
	    ifnull(record_type_gnap_auth,'') = 'mpg_s'
	    AND ifnull(resp_code_num_gnap_auth,0) <> 05 
	
	)
    ) A
JOIN
 ( select * from backup_xl_layer.temp_uk_dif_table_$jbid where card_scheme_diff <> 'OB'
	      and trans_sk not in (select distinct trans_sk from AUTH_DIF_JOIN_IND_ALL_1)
 ) T
        
ON   
	MERCHANT_NUMBER_T = MERCHANT_NUMBER_A
	AND CARD_NBR_T = CARD_NBR_A
	AND
	(
		( TRAN_ID_T = ''  OR  TRAN_ID_A  = '')
		OR ( TRAN_ID_T = TRAN_ID_last_6_1_4_A )
		OR ( TRAN_ID_1_4_T = TRAN_ID_1_4_A )
		OR ( TRAN_ID_T = TRAN_ID_last_4_A)
		OR ( lenght_TRAN_ID_T >= 4)
	)                
                
)

--to ignore cross join from last min pass
,AUTH_DIF_JOIN_IND as ( 
select * from AUTH_DIF_JOIN_IND_ALL_1 
union all 
(
  select * from 
    (select * except (JOININD_JOININDDATE),
            CAST(SPLIT(JOININD_JOININDDATE, '_')[safe_offset(0)] as NUMERIC) as JOININD,
            SPLIT(JOININD_JOININDDATE, '_')[safe_offset(1)] as JOININDDATE  
            from 	AUTH_DIF_JOIN_IND_ALL_2)
      where  JOININD <> 100 	  or 
    concat(trans_sk, NORTH_UK_AUTH_FIN_SK) is null
  ) 
)


, VALIDROW AS 
(
SELECT
   * EXCEPT (trans_sk, trans_sk_original)
   , trans_sk_original AS trans_sk
   , MIN(JOININD) OVER (PARTITION BY NORTH_UK_AUTH_FIN_SK) AS JID
   , MIN(JOININD) OVER (PARTITION BY TRANS_SK) AS JID2

FROM 
  (
      SELECT
        * EXCEPT(trans_sk)
         , trans_sk AS trans_sk_original
         , IFNULL(trans_sk, generate_uuid()) AS trans_sk
      FROM
         AUTH_DIF_JOIN_IND T 
  ) VALIDROW_01  --- TO AVOID NULLS FOR TRANS_SK THAT ARE HARMING PERFORMANCE of PARTITION BY in next query
)
     
---- BEST MATCHING BETWEEN AUTH AND DIF
, AUTH_DIF_JOIN_BASE_01 AS 
(
SELECT
   DISTINCT 
     NORTH_UK_AUTH_FIN_SK AS AUTH_SK
   , TRANS_SK
   , IFNULL(CARD_NUMBER_RK_T,CARD_NUMBER_RK_A) AS CARD_NUMBER_RK
   , CARD_NBR_A as CARD_NUMBER_HK
   , IFNULL(card_number_sk_original_A,card_number_sk_original_T) as card_number_sk_original
   , IFNULL(TRAN_DATE_T,TRAN_DATE_A) AS TRANSACTION_DATE
   , IFNULL(MERCHANT_NUMBER_T,MERCHANT_NUMBER_A) AS MERCHANT_NUMBER
   , IFNULL(AMOUNT_T,AMOUNT_A) AS TRANSACTION_AMOUNT
   , IFNULL(CORP_T,CORP_A) AS CORP
   , IFNULL(REGION_T,REGION_A) AS REGION
   , IFNULL(PRINCIPAL_T,PRINCIPAL_A) AS PRINCIPAL
   , IFNULL(ASSOCIATE_T,ASSOCIATE_A) AS ASSOCIATE
   , IFNULL(CHAIN_T,CHAIN_A) AS CHAIN
   , TRAN_ID_A_ORIGINAL AS BANKNET_TRACE_ID_TRAN
   , MERCHANT_DBA_NAME_A AS MERCHANT_DBA_NAME
   , ifnull(lcot_GUID_KEY_SK_TRAN,lcot_GUID_KEY_SK_AUTH) AS lcot_GUID_KEY_SK  
   , IFNULL(global_trid_diff,global_trid_auth) AS global_trid
   , IFNULL(global_trid_source_diff,global_trid_source_auth) AS global_trid_SOURCE  
   , etlbatchid_tran
   , etlbatchid_auth
   , JOININD
   ,JOININDDATE
   ,global_trid_source_diff
   ,global_trid_source_auth
   ,global_trid_diff
   ,global_trid_auth
   ,global_trid_target
   , ROW_NUMBER() OVER(PARTITION BY TRANS_SK  ORDER BY  NORTH_UK_AUTH_FIN_SK DESC ) AS MAX_ROW
                                                
FROM
    VALIDROW
WHERE
    ( JOININD=JID2 )
    AND ( 
		   ( TRANS_SK IS NOT NULL
			 AND TRANS_SK <> '-1' )
          OR  etlbatchid_auth > (select filter_date_150_days_etlbatchid_auth from backup_xl_layer.uk_auth_dif_filter_dates_$jbid)
        ) 
)


, AUTH_DIF_JOIN_BASE AS 
(
SELECT
    DISTINCT 
	 AUTH_SK
   , TRANS_SK
   , CARD_NUMBER_RK
   , CARD_NUMBER_HK
   , card_number_sk_original
   , TRANSACTION_DATE
   , MERCHANT_NUMBER
   , TRANSACTION_AMOUNT
   , CORP
   , REGION
   , PRINCIPAL
   , ASSOCIATE
   , CHAIN
   , BANKNET_TRACE_ID_TRAN
   , MERCHANT_DBA_NAME
   , lcot_GUID_KEY_SK
   , global_trid
   , global_trid_SOURCE
   , etlbatchid_tran
   , etlbatchid_auth
   , JOININD
   ,JOININDDATE
   ,global_trid_source_diff
   ,global_trid_source_auth
   ,global_trid_diff
   ,global_trid_auth
   ,global_trid_target

from 
    AUTH_DIF_JOIN_BASE_01
WHERE 
   MAX_ROW = 1
)


, VALIDROW_WITH_AUTH_BEFORE_EXCLUDE AS 
(
SELECT
    DISTINCT 
	 A.NORTH_UK_AUTH_FIN_SK AS AUTH_SK
   , TRANS_SK
   , COALESCE(V.CARD_NUMBER_RK,CARD_NUMBER_RK_A,'') AS CARD_NUMBER_RK
   , IFNULL(V.CARD_NUMBER_HK,A.CARD_NBR_A) AS CARD_NUMBER_HK
   , ifnull(card_number_sk_original,card_number_sk_original_A) AS card_number_sk_original
   , IFNULL(V.TRANSACTION_DATE,TRAN_DATE_A) AS TRANSACTION_DATE
   , COALESCE(V.MERCHANT_NUMBER,A.MERCHANT_NUMBER_A,'') AS MERCHANT_NUMBER
   , IFNULL(V.TRANSACTION_AMOUNT,AMOUNT_A) AS TRANSACTION_AMOUNT
   , COALESCE(V.CORP,CORP_A,'') AS CORP
   , COALESCE(V.REGION,REGION_A,'') AS REGION
   , COALESCE(V.PRINCIPAL,PRINCIPAL_A,'') AS PRINCIPAL
   , COALESCE(V.ASSOCIATE,ASSOCIATE_A,'') AS ASSOCIATE
   , COALESCE(V.CHAIN,CHAIN_A,'') AS CHAIN
   , COALESCE(V.BANKNET_TRACE_ID_TRAN,TRAN_ID_A_ORIGINAL,'') AS BANKNET_TRACE_ID_TRAN
   , COALESCE(V.MERCHANT_DBA_NAME,MERCHANT_DBA_NAME_A,'') AS MERCHANT_DBA_NAME               
   , COALESCE(V.lcot_GUID_KEY_SK,lcot_GUID_KEY_SK_AUTH,'') as lcot_GUID_KEY_SK   
   , COALESCE(V.global_trid, A.global_trid_auth,'') as global_trid
   , COALESCE(V.global_trid_SOURCE, A.global_trid_source_auth,'') as global_trid_SOURCE         
   , IFNULL(V.etlbatchid_auth,A.etlbatchid_auth_main) AS etlbatchid_auth
   , etlbatchid_tran               
   , V.JOININD
   , V.JOININDDATE
   ,global_trid_source_diff
   ,COALESCE(V.global_trid_source_auth, A.global_trid_source_auth,'') global_trid_source_auth
   ,global_trid_diff
   ,COALESCE(V.global_trid_auth, A.global_trid_auth,'') global_trid_auth
   ,COALESCE(V.global_trid_target, A.global_trid_target,'') global_trid_target
FROM
    backup_xl_layer.temp_uk_auth_table_$jbid A
LEFT JOIN 
(
    SELECT
      *
    FROM
      AUTH_DIF_JOIN_BASE 
) V

ON
   A.NORTH_UK_AUTH_FIN_SK=V.AUTH_SK                 
)

, VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_ST1 AS 
(SELECT
         VALID.*
      FROM
         VALIDROW_WITH_AUTH_BEFORE_EXCLUDE VALID
      LEFT JOIN 
	    (
            SELECT
              DISTINCT G.TRANS_SK,
              G.AUTH_SK
            FROM
              xl_layer.lcot_uid_key_ukrg G
            WHERE
              (G.AUTH_SK <> '-1')
              AND G.etlbatchid_date > (select filter_date_180_etlbatchid_date from backup_xl_layer.uk_auth_dif_filter_dates_$jbid)
        ) GUID
      ON
         VALID.AUTH_SK = GUID.AUTH_SK
      WHERE
         VALID.TRANS_SK <> '-1'
         OR GUID.TRANS_SK IS NULL )

select * from VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_ST1 
)
;"

lcot_query_4_1="Create or replace table  backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_$jbid AS     --lcot_query_4_1 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh
(
with VALIDROW_UK_KEY_FULL_JOIN AS 
(
SELECT 
   DISTINCT
    IFNULL(VALID_AUTH_LEFT.AUTH_SK,'-1' ) AS AUTH_SK
   , CASE
         WHEN VALID_AUTH_LEFT.TRANS_SK IS NULL THEN ifnull( TRAN_RIGHT.TRANS_SK, '-1')
         ELSE VALID_AUTH_LEFT.TRANS_SK
       END AS TRANS_SK
   , IFNULL( VALID_AUTH_LEFT.CARD_NUMBER_RK,TRAN_RIGHT.CARD_NUMBER_RK ) AS CARD_NUMBER_RK
   , IFNULL( VALID_AUTH_LEFT.CARD_NUMBER_HK,TRAN_RIGHT.CARD_NUMBER_SK ) AS CARD_NUMBER_HK
   , IFNULL( VALID_AUTH_LEFT.card_number_sk_original, TRAN_RIGHT.card_number_sk_original ) as card_number_sk_original
   , IFNULL( VALID_AUTH_LEFT.TRANSACTION_DATE,TRAN_RIGHT.TRANSACTION_DATE ) AS TRANSACTION_DATE
   , IFNULL( VALID_AUTH_LEFT.MERCHANT_NUMBER,TRAN_RIGHT.MERCHANT_NUMBER ) AS MERCHANT_NUMBER
   , IFNULL( VALID_AUTH_LEFT.TRANSACTION_AMOUNT,TRAN_RIGHT.TRANSACTION_AMOUNT ) AS TRANSACTION_AMOUNT
   , IFNULL( VALID_AUTH_LEFT.CORP,TRAN_RIGHT.CORP ) AS CORP
   , IFNULL( VALID_AUTH_LEFT.REGION,TRAN_RIGHT.REGION ) AS REGION
   , IFNULL( VALID_AUTH_LEFT.PRINCIPAL,TRAN_RIGHT.PRINCIPAL ) AS PRINCIPAL
   , IFNULL( VALID_AUTH_LEFT.ASSOCIATE,TRAN_RIGHT.ASSOCIATE ) AS ASSOCIATE
   , IFNULL( VALID_AUTH_LEFT.CHAIN,TRAN_RIGHT.CHAIN ) AS CHAIN
   , IFNULL( VALID_AUTH_LEFT.BANKNET_TRACE_ID_TRAN,TRAN_RIGHT.TRANSACTION_ID ) AS BANKNET_TRACE_ID_TRAN
   , IFNULL( TRAN_RIGHT.MERCHANT_DBA_NAME,VALID_AUTH_LEFT.MERCHANT_DBA_NAME ) AS MERCHANT_DBA_NAME
   , VALID_AUTH_LEFT.lcot_GUID_KEY_SK
   , IFNULL(TRAN_RIGHT.global_trid, VALID_AUTH_LEFT.global_trid) as global_trid
   , IFNULL(TRAN_RIGHT.global_trid_source, VALID_AUTH_LEFT.global_trid_SOURCE) as global_trid_SOURCE
   , IFNULL( VALID_AUTH_LEFT.etlbatchid_tran,TRAN_RIGHT.etlbatchid ) AS etlbatchid_tran
   , etlbatchid_auth
   , JOININD
   ,JOININDDATE
   , IFNULL(TRAN_RIGHT.global_trid_source, VALID_AUTH_LEFT.global_trid_SOURCE) global_trid_source_diff
   ,global_trid_source_auth
   ,IFNULL(TRAN_RIGHT.global_trid, VALID_AUTH_LEFT.global_trid) global_trid_diff
   ,global_trid_auth
   ,global_trid_target

FROM
	backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_ST1_$jbid VALID_AUTH_LEFT --   VALIDROW_UK_KEY VALID_AUTH_LEFT  --- Exclude existing Auth from GUID which has no Dif previously
 
FULL OUTER JOIN 
  (
      SELECT
        T1.*
      FROM 
	   (
          SELECT
              F.ukrg_trans_fact_sk AS TRANS_SK
             , F.CARD_NUMBER_RK
             , cast(F.CARD_NUMBER_SK as string) as CARD_NUMBER_SK
             , cast(F.card_number_sk as string) as card_number_sk_original
             , F.TRANSACTION_DATE
             , F.TRANSACTION_AMOUNT
             , ltrim(F.MERCHANT_NUMBER,'0') as MERCHANT_NUMBER
             , F.corporate AS CORP
             , F.REGION
             , F.PRINCIPAL
             , F.ASSOCIATE
             , F.CHAIN
             , F.transaction_identifier AS TRANSACTION_ID
             , F.MERCHANT_NAME AS MERCHANT_DBA_NAME
			 , F.global_trid
			 , F.global_trid_source
             , F.etlbatchid
          FROM
             (
				select dif.* 
				from 
					xl_layer.vw_ukrg_tran_fact dif join backup_xl_layer.uk_mpg_scorp uk on uk.corporate!=dif.corporate
				where 
					dif.etlbatchid > (select etlbatchid_tran_full_join from backup_xl_layer.uk_auth_dif_filter_dates_$jbid) and (dif.corporate not in ('051','014') and (dif.corporate not in ('052') or dif.REGION <> '05'))  --- 5 days filter 
				qualify row_number() over( partition by ukrg_trans_fact_sk order by etlbatchid DESC ) = 1
			) F
			
	      LEFT JOIN 
			backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_ST1_$jbid VALID
			 
		 ON F.ukrg_trans_fact_sk = VALID.TRANS_SK
          
		 WHERE
             VALID.TRANS_SK IS NULL
       
      ) T1
			
     LEFT OUTER JOIN 
	   (
           SELECT
              DISTINCT TRANS_SK
           FROM
               xl_layer.lcot_uid_key_ukrg L
           WHERE
               etlbatchid_date > (select filter_date_180_etlbatchid_date from backup_xl_layer.uk_auth_dif_filter_dates_$jbid)
               AND L.TRANS_SK <> '-1'
       ) T2
     ON
         T1.TRANS_SK=T2.TRANS_SK
     WHERE
         T2.TRANS_SK IS NULL
  ) TRAN_RIGHT

ON
    VALID_AUTH_LEFT.TRANS_SK = TRAN_RIGHT.TRANS_SK
)

select * from VALIDROW_UK_KEY_FULL_JOIN
)
;"
                                
lcot_query_5="create or replace table backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_UNION_$jbid as

(
---******************************************
--******************************************
-- logic to retain old GUID SKIP

with VALID_KEY_TABLE_DATA_GUID_SK AS 
(
SELECT
    IFNULL(UK_TRANS_GUID.lcot_GUID_KEY_SK_TRANS, UK_AUTH_GUID.lcot_GUID_KEY_SK_AUTH ) AS lcot_GUID_KEY_SK
   , IFNULL(VALID.AUTH_SK,'-1') AS AUTH_SK
   , IFNULL(VALID.TRANS_SK,'-1') AS TRANS_SK
   , IFNULL(UK_TRANS_GUID.TRANIN_SETTLED_SK,'-1') AS TRANIN_SETTLED_SK
   , card_number_sk_original
   , CARD_NUMBER_RK
   , CARD_NUMBER_HK
   , TRANSACTION_DATE
   , MERCHANT_NUMBER
   , TRANSACTION_AMOUNT
   , CORP
   , REGION
   , PRINCIPAL
   , ASSOCIATE
   , CHAIN
   , BANKNET_TRACE_ID_TRAN
   , MERCHANT_DBA_NAME
   , UK_TRANS_GUID.ACQ_REFERENCE_NUMBER
   , global_trid
   , global_trid_SOURCE
   , etlbatchid_auth
   , etlbatchid_tran
   , JOININD
   ,JOININDDATE
   ,global_trid_source_diff
   ,global_trid_source_auth
   ,global_trid_diff
   ,global_trid_auth
   ,global_trid_target
FROM
    backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_$jbid VALID
	
LEFT JOIN 
  (
        SELECT
          lcot_GUID_KEY_SK AS lcot_GUID_KEY_SK_TRANS
          , G.TRANS_SK
          , ifnull(G.TRANIN_SETTLED_SK,'-1') AS TRANIN_SETTLED_SK 
          , G.ACQ_REFERENCE_NUMBER
        FROM
           xl_layer.lcot_uid_key_ukrg G
        WHERE
           G.TRANS_SK <> '-1'
           AND G.AUTH_SK = '-1'
           AND G.etlbatchid_date > (select filter_date_180_etlbatchid_date from backup_xl_layer.uk_auth_dif_filter_dates_$jbid)
  ) UK_TRANS_GUID
  ON
    VALID.TRANS_SK=UK_TRANS_GUID.TRANS_SK

LEFT JOIN 
  (
      SELECT
          G.lcot_GUID_KEY_SK AS lcot_GUID_KEY_SK_AUTH
         , G.AUTH_SK
      FROM
         xl_layer.lcot_uid_key_ukrg G
      WHERE
         G.AUTH_SK <> '-1'
         AND G.TRANS_SK = '-1'
         AND G.etlbatchid_date > (select filter_date_180_etlbatchid_date from backup_xl_layer.uk_auth_dif_filter_dates_$jbid)
  ) UK_AUTH_GUID
  ON
     VALID.AUTH_SK=UK_AUTH_GUID.AUTH_SK
)
     
, VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_01 AS
(
SELECT
    lcot_GUID_KEY_SK 
   , ROW_NUMBER() OVER(PARTITION BY IFNULL(lcot_GUID_KEY_SK,generate_uuid()) ORDER BY CURRENT_DATE ASC) AS ROW_NUM
   , AUTH_SK
   , TRANS_SK
   , TRANIN_SETTLED_SK
   , CARD_NUMBER_RK
   , CARD_NUMBER_HK
   , card_number_sk_original
   , TRANSACTION_DATE
   , MERCHANT_NUMBER
   , TRANSACTION_AMOUNT
   , CORP
   , REGION
   , PRINCIPAL
   , ASSOCIATE
   , CHAIN
   , BANKNET_TRACE_ID_TRAN
   , MERCHANT_DBA_NAME
   , ACQ_REFERENCE_NUMBER
   , global_trid
   , global_trid_SOURCE
   , etlbatchid_auth
   , etlbatchid_tran
   , JOININD
   ,JOININDDATE
   ,global_trid_source_diff
   ,global_trid_source_auth
   ,global_trid_diff
   ,global_trid_auth
   ,global_trid_target
FROM
   VALID_KEY_TABLE_DATA_GUID_SK VALID 
)

select 
    case when VALID.ROW_NUM=1 AND VALID.lcot_GUID_KEY_SK IS NOT NULL then VALID.lcot_GUID_KEY_SK
    else generate_uuid() end as lcot_GUID_KEY_SK
    , VALID.AUTH_SK
    , VALID.TRANS_SK
    , VALID.TRANIN_SETTLED_SK
    , VALID.CARD_NUMBER_RK
    , VALID.CARD_NUMBER_HK
	, VALID.CARD_NUMBER_SK_ORIGINAL
    , VALID.TRANSACTION_DATE
    , VALID.MERCHANT_NUMBER
    , VALID.TRANSACTION_AMOUNT
    , VALID.CORP
    , VALID.REGION
    , VALID.PRINCIPAL
    , VALID.ASSOCIATE
    , VALID.CHAIN
    , VALID.BANKNET_TRACE_ID_TRAN
    , VALID.MERCHANT_DBA_NAME
    , VALID.ACQ_REFERENCE_NUMBER
	, VALID.global_trid
    , VALID.global_trid_SOURCE
    , VALID.etlbatchid_auth
    , VALID.etlbatchid_tran
    , JOININD
   ,JOININDDATE
   ,global_trid_source_diff
   ,global_trid_source_auth
   ,global_trid_diff
   ,global_trid_auth
   ,global_trid_target
from VALID_KEY_TABLE_DATA_GUID_SK_ROW_NUM_01 VALID
where (VALID.ROW_NUM=1
    AND VALID.lcot_GUID_KEY_SK IS NOT NULL)
    or
    (VALID.ROW_NUM <> 1 OR lcot_GUID_KEY_SK IS NULL)
   
);"



lcot_query_6=" CREATE OR REPLACE TABLE backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_FUNCTION_CALL_$jbid AS
(    --lcot_query_6 lcot_uid_ukrg_auth uk_lcot_guid_auth_dif.sh

---******************************************
--******************************************
-- Below code added for FUNCTION CALL to Populate Customer_unique_reference_number

with VALID_KEY_TABLE_DATA_FUNCTION_CALL AS 
(
  
	SELECT
	global_trid,
    global_trid_SOURCE,
    xl_layer.get_customer_unique_reference_number(global_trid,global_trid_SOURCE, MAX(cast(host_key_application.application_partition_filter as int64)), application_field) as customer_unique_reference_number, 
	CORP,
    REGION,
    PRINCIPAL,
    ASSOCIATE,
    CHAIN
  FROM
	backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_UNION_$jbid Valid_func
  LEFT JOIN
    trusted_layer.dim_partner_identification dim
  ON
    (
      case 
  WHEN dim.host_key_column = 'chain' AND CONCAT(Valid_func.CORP,Valid_func.REGION,Valid_func.PRINCIPAL,Valid_func.ASSOCIATE,Valid_func.CHAIN) = dim.host_key_value THEN 1
  WHEN dim.host_key_column = 'associate' AND
  CONCAT(Valid_func.CORP,Valid_func.REGION,Valid_func.PRINCIPAL,Valid_func.ASSOCIATE) = dim.host_key_value 
  THEN 1
  WHEN dim.host_key_column = 'principal' AND 
  CONCAT(Valid_func.CORP,Valid_func.REGION,Valid_func.PRINCIPAL) = dim.host_key_value
  THEN 1
  WHEN dim.host_key_column = 'region' AND 
  CONCAT(Valid_func.CORP,Valid_func.REGION) = dim.host_key_value 
  THEN 1
  WHEN dim.host_key_column = 'corporate' AND Valid_func.CORP = dim.host_key_value 
  THEN 1 
 ELSE 0
END = 1   )
  CROSS JOIN
    UNNEST (host_key_application) host_key_application
  WHERE
    host_key_application.application_name='fn_lookup_customer_unique_reference'
    AND (global_trid IS NOT NULL
      AND global_trid <> '')
  GROUP BY
    Valid_func.CORP,
    Valid_func.REGION,
    Valid_func.PRINCIPAL,
    Valid_func.ASSOCIATE,
    Valid_func.CHAIN,
    host_key_application.application_field,
    global_trid,
    global_trid_SOURCE
)
SELECT 
    lcot_GUID_KEY_SK
   , AUTH_SK
   , TRANS_SK
   , TRANIN_SETTLED_SK
   , CARD_NUMBER_RK
   , CARD_NUMBER_HK
   , CARD_NUMBER_SK_ORIGINAL
   , TRANSACTION_DATE
   , MERCHANT_NUMBER
   , TRANSACTION_AMOUNT
   , VALID.CORP
   , VALID.REGION
   , VALID.PRINCIPAL
   , VALID.ASSOCIATE
   , VALID.CHAIN
   , BANKNET_TRACE_ID_TRAN
   , MERCHANT_DBA_NAME
   , ACQ_REFERENCE_NUMBER
   , VALID.global_trid
   , VALID.global_trid_SOURCE
   , ETLBATCHID_AUTH
   , ETLBATCHID_TRAN
   , func.customer_unique_reference_number
   , JOININD JOININD_auth_dif
   ,JOININDDATE JOININDDATE_auth_dif
   ,global_trid_source_diff global_trid_source_dif
   ,global_trid_source_auth global_trid_source_tds
   ,global_trid_diff global_trid_dif
   ,global_trid_target global_trid_tds
    FROM backup_xl_layer.VALID_KEY_TABLE_DATA_GUID_SK_UNION_$jbid VALID
    LEFT JOIN
    VALID_KEY_TABLE_DATA_FUNCTION_CALL func
    on VALID.global_trid = func.global_trid
    and VALID.global_trid_SOURCE = func.global_trid_SOURCE
    and CONCAT(VALID.CORP,VALID.REGION,VALID.PRINCIPAL,VALID.ASSOCIATE,VALID.CHAIN) = CONCAT(func.CORP,func.REGION,func.PRINCIPAL,func.ASSOCIATE,func.CHAIN)
);"

echo "*************** Calling generic function for bq tables to run ********************"

bq_run "${lcot_query_1}" 
bq_run "${lcot_query_1_1}"
bq_run "${lcot_query_2}" 
bq_run "${lcot_query_3}" 
bq_run "${lcot_query_4}" 
bq_run "${lcot_query_4_1}" 
bq_run "${lcot_query_5}" 
bq_run "${lcot_query_6}" 
echo "JobId Array= ${jbid_arr[*]}"