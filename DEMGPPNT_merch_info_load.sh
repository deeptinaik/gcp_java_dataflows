#!/bin/bash
set -o pipefail

global_var=""
source_etlbatchid=$1
etlbatchid=$2


if [ $# -ne 2 ]
then
  echo -e "Parameters incorrect expected parameters for variable etlbatchid\n"
  exit 1
fi


bq_run()
{
	QUERY=$1

	jobID="youcm_gppn_load-"`date '+%Y%m%d%H%M%S%3N'`

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
		global_var="$number_of_rows_affected "

	else
		echo -ne "{\\\"jobid\\\" : \\\"$jobID\\\",\\\"error\\\" : \\\"$execution_output\\\"}"|tr -d '\n'|tr "'"		"*"
		exit 2
	fi
else
	echo -e "\n****************** Query Validation Failed.... Cause: $dry_run_output ******************\n"
	exit 1
fi
}

upsert_run()
{
	QUERY=$1

	jobID="youcm_gppn_load_update_max-"`date '+%Y%m%d%H%M%S%3N'`

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

	else
		echo -ne "{\\\"jobid\\\" : \\\"$jobID\\\",\\\"error\\\" : \\\"$execution_output\\\"}"|tr -d '\n'|tr "'"		"*"
		exit 2
	fi
else
	echo -e "\n****************** Query Validation Failed.... Cause: $dry_run_output ******************\n"
	exit 1
fi
}

load_temp_table="CREATE OR REPLACE TABLE
  backup_cnsmp_layer.dimension_merch_info_temp AS(
  SELECT
    *
  FROM
    cnsmp_layer.dimension_merch_info
  WHERE
    current_ind='0'
    and purge_flag = 'N'
    AND etlbatchid>${source_etlbatchid} )"

gpn_temp_table_load="CREATE OR REPLACE TABLE
    backup_cnsmp_layer_sensitive.youcm_gppn_temp AS(
  SELECT
    *,
    CASE
      WHEN IFNULL(cdc_hash_key,'')=IFNULL(change_data_capture_hash,'') THEN 0
      ELSE 1
  END
    AS update_insert_flag,
  FROM (
    SELECT
      etlbatchid,
      merchant_information_sk AS merchant_sequence_key,
      hierarchy,
      merchant_number,
      dba_name,
      legal_name,
      company_name,
      retail_name,
      additional_optional_data,
      contact_array,
      email_array,
      phone_number_array,
      dda_array,
      tax_identification_number AS company_tax_id,
      LPAD(SUBSTR(TRIM(tax_identification_number),-4),LENGTH(TRIM(tax_identification_number)),'X') AS company_tax_id_rk,
      owner_information_array,
      geographical_region,
      TO_BASE64(MD5(CONCAT(IFNULL(hierarchy, ''),IFNULL(merchant_number, ''),(IFNULL(dba_name, '')),(IFNULL(legal_name, '')),(IFNULL(merchant_contact_name, '')), (IFNULL(primary_email_address, '')), (IFNULL(secondary_email_address, '')), (IFNULL(officer_phone_number, '')), (IFNULL(dba_phone_number, '')), (IFNULL(CAST(dda_array_unnest[
                OFFSET
                  (0)] AS string), '')), (IFNULL(CAST(dda_array_unnest[
                OFFSET
                  (1)] AS string), '')), (IFNULL(CAST(dda_array_unnest[
                OFFSET
                  (2)] AS string), '')), (IFNULL(CAST(dda_array_unnest[
                OFFSET
                  (3)] AS string), '')), (IFNULL(CAST(tax_identification_number AS string), '')), (IFNULL(CAST(owner_primary_id_information_array_unnest[
                OFFSET
                  (0)] AS string), '')), (IFNULL(CAST(owner_primary_id_information_array_unnest[
                OFFSET
                  (1)] AS string), '')), (IFNULL(CAST(owner_name_information_array_unnest[
                OFFSET
                  (0)] AS string), '')), (IFNULL(CAST(owner_name_information_array_unnest[
                OFFSET
                  (1)] AS string), '')), (IFNULL(CAST(owner_primary_sec_id_rk_information_array_unnest[
                OFFSET
                  (0)] AS string), '')), (IFNULL(CAST(owner_primary_sec_id_rk_information_array_unnest[
                OFFSET
                  (1)] AS string), '')),(IFNULL(merchant_status_code, '')), (IFNULL(dba_address1, '')), (IFNULL(dba_address2, '')), (IFNULL(dba_city, '')), (IFNULL(dba_state, '')), (IFNULL(dba_country_code, '')), (IFNULL(dba_zip, '')) ))) AS change_data_capture_hash,
      cdc_hash_key,
      merchant_status_code AS merchant_status,
      purge_flag,
      dba_address_array
    FROM (
      SELECT
        etlbatchid,
        merchant_information_sk,
        hierarchy,
        merchant_number,
        dba_name,
        legal_name,
        company_name,
        retail_name,
        additional_optional_data,
        merchant_contact_name,
        primary_email_address,
        secondary_email_address,
        officer_phone_number,
        dba_phone_number,
        phone_number_array,
        dda_array,
        owner_information_array,
        email_array,
        contact_array,
        geographical_region,
        ARRAY(
        SELECT
          IFNULL(dda_nbr,' ')
        FROM
          UNNEST(dda_array)
        ORDER BY
          sequence_number) AS dda_array_unnest,
        ARRAY(
        SELECT
          IFNULL(dda_number_rk,' ')
        FROM
          UNNEST(dda_array)
        ORDER BY
          sequence_number) AS dda_number_rk_array_unnest,
        ARRAY(
        SELECT
          IFNULL(owner_name,' ')
        FROM
          UNNEST(owner_information_array)
        ORDER BY
          sequence_number) AS owner_name_information_array_unnest,
        ARRAY(
        SELECT
          IFNULL(primary_id,' ')
        FROM
          UNNEST(owner_information_array)
        ORDER BY
          sequence_number) AS owner_primary_id_information_array_unnest,
        ARRAY(
        SELECT
          IFNULL(primary_sec_id_rk,' ')
        FROM
          UNNEST(owner_information_array)
        ORDER BY
          sequence_number) AS owner_primary_sec_id_rk_information_array_unnest,
        tax_identification_number,
        cdc_hash_key,
        merchant_status_code,
        purge_flag,
        dba_address_array,
        dba_address1,
        dba_address2,
        dba_city,
        dba_state,
        dba_country_code,
        dba_zip
      FROM (
        SELECT
          mt.etlbatchid,
          TRIM(mt.merchant_number) AS merchant_number,
          TRIM(mt.hierarchy) AS hierarchy,
          TRIM(mt.dba_name) AS dba_name,
          TRIM(mt.legal_name) AS legal_name,
          TRIM(mt.company_name) AS company_name,
          TRIM(mt.retail_name) AS retail_name,
          TRIM(mt.additional_optional_data) AS additional_optional_data,
          TRIM(mt.merchant_contact_name) AS merchant_contact_name,
          TRIM(mt.primary_email_address) AS primary_email_address,
          TRIM(mt.secondary_email_address) AS secondary_email_address,
          TRIM(mt.officer_phone_number) AS officer_phone_number,
          TRIM(mt.dba_phone_number) AS dba_phone_number,
          TRIM(mt.merchant_information_sk) AS merchant_information_sk,
          TRIM(mt.merchant_status) AS merchant_status,
          TRIM(mt.purge_flag) AS purge_flag,
          TRIM(mt.dba_address1) AS dba_address1,
          TRIM(mt.dba_address2) AS dba_address2,
          TRIM(mt.dba_city) AS dba_city,
          TRIM(mt.dba_state) AS dba_state,
          TRIM(mt.dba_country_ind) AS dba_country_code,
          TRIM(mt.dba_postal_code) AS dba_zip,
          (
          SELECT
            ARRAY_AGG(STRUCT(CAST(dda.sequence_number AS string) AS sequence_number,
                dda.dda_nbr,
                dda.dda_number_rk))
          FROM (
            SELECT
              DISTINCT TRIM(dim.merchant_number) AS merchant_number,
              TRIM(tl.dda_nbr) AS dda_nbr,
              sequence_number,
              dim.dda_hash,
              case when LENGTH(TRIM(tl.dda_nbr))>4 then
              LPAD(SUBSTR(TRIM(tl.dda_nbr),-4),LENGTH(TRIM(tl.dda_nbr)),'X')
              when LENGTH(TRIM(tl.dda_nbr))<5 then right('0000'||TRIM(tl.dda_nbr), 4)
              end AS dda_number_rk
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp
            UNPIVOT
              INCLUDE NULLS (dda_hash FOR sequence_number IN (dda1_last4_hash_key AS 1,
                  dda2_last4_hash_key AS 2,
                  dda3_last4_hash_key AS 3,
                  dda4_last4_hash_key AS 4) )dim
            LEFT JOIN (
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ukrg_mmfndeu_supplement_funding_sk) AS ukrg_mmfndeu_supplement_funding_sk,
                etlbatchid,
                hash_key_value,
                'ukrg'
              FROM
                xl_layer.ukrg_mmfndeu_supplement_funding
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ap_mmddsq_dda_tbl_sk) AS ap_mmddsq_dda_tbl_sk,
                etlbatchid,
                hash_key_value,
                'AP'
              FROM
                xl_layer.ap_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ca_mmddsq_dda_tbl_sk) AS ca_mmddsq_dda_tbl_sk,
                etlbatchid,
                hash_key_value,
                'CA'
              FROM
                xl_layer.ca_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(us_mmddsq_dda_tbl_sk) AS us_mmddsq_dda_tbl_sk,
                etlbatchid,
                hash_key_value,
                'US'
              FROM
                xl_layer.us_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(indir_mmddsq_dda_tbl_sk) AS indir_mmddsq_dda_tbl_sk,
                etlbatchid,
                hash_key_value,
                'IND'
              FROM
                xl_layer.indir_mmddsq_dda_tbl ) xl
            ON
              TRIM(dim.merchant_number)=TRIM(xl.merchant_number)
              AND dim.dda_hash=xl.hash_key_value
            LEFT JOIN (
              SELECT
                TRIM(dda_nbr) AS dda_nbr,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ukrg_mmfndeu_supplement_funding
              UNION ALL
              SELECT
                TRIM(nbr) AS nbr,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ap_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(nbr) AS nbr,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ca_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(nbr) AS nbr,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.us_mmddsq_dda_tbl
              UNION ALL
              SELECT
                TRIM(nbr) AS nbr,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.indir_mmddsq_dda_tbl ) tl
            ON
              (xl.hash_key_value=tl.hash_key_value
                AND tl.etlbatchid=CAST(xl.etlbatchid AS string))
            WHERE
              dim.current_ind='0' ) dda
          WHERE
            TRIM(dda.merchant_number)=TRIM(mt.merchant_number) ) AS dda_array,
          (
          SELECT
            ARRAY_AGG(STRUCT(con.sequence_number AS sequence_number,
                con.contact_name))
          FROM (
            SELECT
              DISTINCT TRIM(merchant_number) AS merchant_number,
              TRIM(contact_name) AS contact_name,
              sequence_number
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp
            UNPIVOT
              INCLUDE NULLS (contact_name FOR sequence_number IN (merchant_contact_name AS 1 )) ) con
          WHERE
            TRIM(con.merchant_number)=TRIM(mt.merchant_number) ) AS contact_array,
          (
          SELECT
            ARRAY_AGG(STRUCT(CAST(em.sequence_number AS string) AS sequence_number,
                em.email_address))
          FROM (
            SELECT
              DISTINCT TRIM(merchant_number) AS merchant_number,
              TRIM(email_address) AS email_address,
              sequence_number
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp
            UNPIVOT
              INCLUDE NULLS (email_address FOR sequence_number IN (primary_email_address AS 1,
                  secondary_email_address AS 2)) ) em
          WHERE
            TRIM(em.merchant_number)=TRIM(mt.merchant_number) ) AS email_array,
          (
          SELECT
            ARRAY_AGG(STRUCT(CAST(em.phone_number_ind AS string) AS phone_number_ind,
                em.phone_number))
          FROM (
            SELECT
              DISTINCT TRIM(merchant_number) AS merchant_number,
              TRIM(phone_number) AS phone_number,
              TRIM(phone_number_ind) AS phone_number_ind
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp
            UNPIVOT
              INCLUDE NULLS (phone_number FOR phone_number_ind IN (officer_phone_number AS 'DBA',
                  dba_phone_number AS 'ADDITIONAL')) ) em
          WHERE
            TRIM(em.merchant_number)=TRIM(mt.merchant_number) ) AS phone_number_array,
          (
          SELECT
            ARRAY_AGG(STRUCT(CAST(own.sequence_number AS string) AS sequence_number,
                own.owner_name,
                own.primary_id,
                own.primary_sec_id_rk))
          FROM (
            SELECT
              DISTINCT TRIM(dim.merchant_number) AS merchant_number,
              TRIM(tl.primary_id) AS primary_id,
              sequence_number,
              TRIM(owner_name) AS owner_name,
              TRIM(dim.primary_sec_id_rk) AS primary_sec_id_rk
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp
            UNPIVOT
              INCLUDE NULLS ( (owner_name,
                  primary_id_hash,
                  primary_sec_id_rk) FOR sequence_number IN ((primary_owner_name,
                    primary_id_hash_key,
                    primary_id_rk) AS 1,
                  (secondary_owner_name,
                    secondary_primary_id_hash_key,
                    secondary_primary_id_rk ) AS 2) ) dim
            LEFT JOIN (
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ukrg_mmownq_own_sk) AS ukrg_mmownq_own_sk,
                etlbatchid,
                hash_key_value
              FROM
                xl_layer.ukrg_mmownq_own
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ap_mmownq_own_sk) AS ap_mmownq_own_sk,
                etlbatchid,
                hash_key_value
              FROM
                xl_layer.ap_mmownq_own
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ca_mmownq_own_sk) AS ca_mmownq_own_sk,
                etlbatchid,
                hash_key_value
              FROM
                xl_layer.ca_mmownq_own
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(us_mmownq_own_sk) AS us_mmownq_own_sk,
                etlbatchid,
                hash_key_value
              FROM
                xl_layer.us_mmownq_own
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(indir_mmownq_own_sk) AS indir_mmownq_own_sk,
                etlbatchid,
                hash_key_value
              FROM
                xl_layer.indir_mmownq_own ) xl
            ON
              TRIM(dim.merchant_number)=TRIM(xl.merchant_number)
              AND dim.primary_id_hash=xl.hash_key_value
            LEFT JOIN (
              SELECT
                TRIM(primary_id) AS primary_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ukrg_mmownq_own
              UNION ALL
              SELECT
                TRIM(primary_id) AS primary_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ap_mmownq_own
              UNION ALL
              SELECT
                TRIM(primary_id) AS primary_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ca_mmownq_own
              UNION ALL
              SELECT
                TRIM(primary_id) AS primary_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.us_mmownq_own
              UNION ALL
              SELECT
                TRIM(primary_id) AS primary_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.indir_mmownq_own ) tl
            ON
              (xl.hash_key_value=tl.hash_key_value
                AND tl.etlbatchid=CAST(xl.etlbatchid AS string)) ) own
          WHERE
            TRIM(own.merchant_number)=TRIM(mt.merchant_number) ) AS owner_information_array,
          (
          SELECT
            ARRAY_AGG(STRUCT(address.dba_address1 AS dba_address1,
                address.dba_address2 AS dba_address2,
                address.dba_city AS dba_city,
                address.dba_state AS dba_state,
                address.dba_country_ind AS dba_country_ind,
                address.dba_postal_code AS dba_postal_code ))
          FROM (
            SELECT
              DISTINCT TRIM(dba_address1) AS dba_address1,
              TRIM(dba_address2) AS dba_address2,
              TRIM(dba_city) AS dba_city,
              TRIM(dba_state) AS dba_state,
              TRIM(dba_country_ind) AS dba_country_ind,
              TRIM(dba_postal_code) AS dba_postal_code,
              TRIM(merchant_number) AS merchant_number
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp ) address
          WHERE
            TRIM(address.merchant_number)=TRIM(mt.merchant_number)) AS dba_address_array,
          (
          SELECT
            DISTINCT TRIM(company_tax_id) AS company_tax_id
          FROM (
            SELECT
              TRIM(tl.company_tax_id) AS company_tax_id,
              TRIM(dim.merchant_number) AS merchant_number
            FROM
              backup_cnsmp_layer.dimension_merch_info_temp dim
            LEFT JOIN (
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ukrg_mmidxsq_idx_sk) AS ukrg_mmidxsq_idx_sk,
                etlbatchid,
                hash_key_value,
                ROW_NUMBER() OVER (PARTITION BY hash_key_value ORDER BY etlbatchid DESC) AS rn
              FROM
                xl_layer.ukrg_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ap_mmidxsq_idx_sk) AS ap_mmidxsq_idx_sk,
                etlbatchid,
                hash_key_value,
                ROW_NUMBER() OVER (PARTITION BY hash_key_value ORDER BY etlbatchid DESC) AS rn
              FROM
                xl_layer.ap_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(ca_mmidxsq_idx_sk) AS ca_mmidxsq_idx_sk,
                etlbatchid,
                hash_key_value,
                ROW_NUMBER() OVER (PARTITION BY hash_key_value ORDER BY etlbatchid DESC) AS rn
              FROM
                xl_layer.ca_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(us_mmidxsq_idx_sk) AS us_mmidxsq_idx_sk,
                etlbatchid,
                hash_key_value,
                ROW_NUMBER() OVER (PARTITION BY hash_key_value ORDER BY etlbatchid DESC) AS rn
              FROM
                xl_layer.us_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(merchant_number) AS merchant_number,
                TRIM(indir_mmidxsq_idx_sk) AS indir_mmidxsq_idx_sk,
                etlbatchid,
                hash_key_value,
                ROW_NUMBER() OVER (PARTITION BY hash_key_value ORDER BY etlbatchid DESC) AS rn
              FROM
                xl_layer.indir_mmidxsq_idx ) xl
            ON
              TRIM(dim.merchant_number)=TRIM(xl.merchant_number)
              AND xl.rn=1
              AND dim.tax_identification_number_hash_key=xl.hash_key_value
            JOIN (
              SELECT
                TRIM(company_tax_id) AS company_tax_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ukrg_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(company_tax_id) AS company_tax_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ap_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(company_tax_id) AS company_tax_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.ca_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(company_tax_id) AS company_tax_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.us_mmidxsq_idx
              UNION ALL
              SELECT
                TRIM(company_tax_id) AS company_tax_id,
                etlbatchid,
                hash_key_value
              FROM
                tl_layer_sens.indir_mmidxsq_idx ) tl
            ON
              (xl.hash_key_value=tl.hash_key_value
                AND tl.etlbatchid=CAST(xl.etlbatchid AS string)) ) tax_no
          WHERE
            TRIM(tax_no.merchant_number)=TRIM(mt.merchant_number) ) AS tax_identification_number,
            geographical_region,
          CASE
            WHEN UPPER(TRIM(COALESCE(merchant_status,''))) IN ('D', 'C', '', NULL) THEN 'CLOSED'
            WHEN UPPER(TRIM(COALESCE(merchant_status,''))) IN ('F' )THEN 'INACTIVE'
            WHEN UPPER(TRIM(COALESCE(merchant_status,''))) IN ('O', 'R') THEN 'OPEN'
            WHEN UPPER(TRIM(COALESCE(merchant_status,''))) IN ( 'P') THEN 'PENDING'
            ELSE 'UNKNOWN'
        END
          AS merchant_status_code,
          (
          SELECT
            cdc_hash_key
          FROM (
            SELECT
              DISTINCT TRIM(merchant_number) AS merchant_number,
              cdc_hash_key
            FROM
              cnsmp_layer_sensitive.master_merch_info
            WHERE
              current_ind=0
              AND acquirer_name='GPN') cdc_tgt_curr
          WHERE
            TRIM(cdc_tgt_curr.merchant_number)=TRIM(mt.merchant_number) ) AS cdc_hash_key
        FROM
          backup_cnsmp_layer.dimension_merch_info_temp mt
          LEFT JOIN (
          SELECT
            DISTINCT corporate,
            region,
            geographical_region
          FROM
            cnsmp_layer.merchant_geo_location) a
        ON
          a.corporate = mt.corporate
          AND a.region = mt.region))))"

insert_table_data="MERGE INTO
  cnsmp_layer_sensitive.master_merch_info TARGET
USING
  (
  SELECT
    GENERATE_UUID() AS master_merch_info_sk,
    etlbatchid,
    parse_date('%Y%m%d',substr(cast(etlbatchid as string),0,8)) as etl_batch_date,
    CURRENT_DATETIME() AS create_date_time,
    CURRENT_DATETIME() AS update_date_time,
    merchant_sequence_key AS merchant_sequence_key,
    hierarchy,
    merchant_number,
    dba_name,
    legal_name,
    company_name,
    retail_name,
    additional_optional_data,
    contact_array,
    email_array,
    phone_number_array,
    dda_array,
    company_tax_id,
    company_tax_id_rk,
    owner_information_array,
    '' AS owner_phone_number,
    'GPN' AS acquirer_name,
    case
    when geographical_region is null then 'UN'
    when geographical_region is not null then geographical_region
    end as geographical_region,
    merchant_status,
    purge_flag,
    dba_address_array,
    change_data_capture_hash,
    update_insert_flag
  FROM
    backup_cnsmp_layer_sensitive.youcm_gppn_temp temp_src ) SOURCE
ON
  source.merchant_number=target.merchant_number
  AND IFNULL(target.cdc_hash_key,'')=IFNULL(source.change_data_capture_hash,'')
  WHEN NOT MATCHED
  THEN
INSERT
VALUES (
  GENERATE_UUID(),
   etlbatchid,
   parse_date('%Y%m%d',substr(cast(etlbatchid as string),0,8)),
   CURRENT_DATETIME(),
   CURRENT_DATETIME(),
   merchant_sequence_key,
   hierarchy,
   merchant_number,
   dba_name,
   legal_name,
   company_name,
   retail_name,
   additional_optional_data,
   contact_array,
   email_array,
   phone_number_array,
   dda_array,
   company_tax_id,
   company_tax_id_rk,
   owner_information_array,
   owner_phone_number,
   acquirer_name,
   geographical_region,
   0,
   change_data_capture_hash,
   merchant_status,
   purge_flag,
   dba_address_array
   )"

#Query in case merchant is having 'Y' in dimension_merch_info
query_2="update cnsmp_layer_sensitive.master_merch_info
set purge_flag = 'Y' where merchant_number in (
  select merchant_number from cnsmp_layer.dimension_merch_info
  where purge_flag = 'Y'
) and  acquirer_name = 'GPN'"

#Query in case merchant is having 'N' in dimension_merch_info
query_3="update cnsmp_layer_sensitive.master_merch_info
set purge_flag = 'N' where merchant_number in (
  select merchant_number from cnsmp_layer.dimension_merch_info
  where purge_flag = 'N'
) and acquirer_name = 'GPN'"

update_current_ind="MERGE
  cnsmp_layer_sensitive.master_merch_info TARGET
USING
  (
  SELECT
    change_data_capture_hash,
    cdc_hash_key,
    merchant_number,
    update_insert_flag
  FROM
    backup_cnsmp_layer_sensitive.youcm_gppn_temp ) SOURCE
ON
  source.merchant_number=target.merchant_number
  AND source.cdc_hash_key=target.cdc_hash_key
  AND source.update_insert_flag=1
  WHEN MATCHED
  THEN
UPDATE
SET
  target.current_ind=1"

get_max_etlbatchid="UPDATE
  configuration.max_etl_config
SET
  max_etlbatchid= (
  SELECT
    IFNULL(MAX(DISTINCT etlbatchid),${source_etlbatchid})
  FROM
    backup_cnsmp_layer_sensitive.youcm_gppn_temp )
WHERE
  process='GPN'
  "

bq_run "${load_temp_table}"
bq_run "${gpn_temp_table_load}"
bq_run "${insert_table_data}"
bq_run "${query_2}"
bq_run "${query_3}"
upsert_run "${update_current_ind}"
upsert_run "${get_max_etlbatchid}"

echo $global_var
