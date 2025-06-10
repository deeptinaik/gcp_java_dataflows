package com.globalpayments.batch.pipeline.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryTranslator implements SerializableFunction<String, String>
{
	public static final Logger LOG = LoggerFactory.getLogger(QueryTranslator.class);
	private static final long serialVersionUID = -8756136035138646831L;
	/**
	 * Read data from query with respect to etlbatchid 
	 */

	String tableName;

	public QueryTranslator(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String apply(String batchId) 
	{
		StringBuilder queryToExecute = new StringBuilder();
		
		String dateFromBatchId = LocalDate.parse(batchId.substring(0, 8), DateTimeFormatter.ofPattern(Constants.INPUT_DATE_FORMAT ))
				.format(DateTimeFormatter.ofPattern(Constants.DATE_FORMAT));
		
		//if (batchId.equals("-1"))
			//return String.format("SELECT * from `%s`", tableName);
		//else
			//return String.format("SELECT * from `%s` where etlbatchid = '%s' and etl_batch_date= '%s';", tableName, batchId,dateFromBatchId);
		// Added Calculation of SHA-512 Hash Key based on columns pulled from trusted
		LOG.info(queryToExecute.toString());
		return queryToExecute.append("select temptbl.* ,\r\n" + 
				"dm_avs_res_cd.avs_response_code_sk,\r\n" + 
				"dm_auth_src.authorization_source_code_desc ,\r\n" + 
				"dm_crd_id_mtd.cardholder_id_method_desc,\r\n" + 
				"dm_mt_rc_mp.moto_ec_ind_short_desc,\r\n" + 
				"dm_mt_rc_mp.moto_ec_ind_long_desc,\r\n" + 
				"dm_pos_enty.pos_entry_mode_desc as pos_entry_code_desc,\r\n" +
				"dm_pos_enty.pos_entry_mode_short_desc as pos_entry_mode_short_desc,\r\n" +
				"dm_trm_cap_code.terminal_capability_code_desc,\r\n" + 
				"dm_visa_itrchng.visa_interchange_level_desc,\r\n" + 
				"dm_mc_itrchng.mc_interchange_level_desc,\r\n" + 
				"dm_mc_dvc_typ.mc_device_type_code_desc,\r\n" + 
				"case when (dm_src.source_sk)is null then \"-2\" else dm_src.source_sk end as source_sk\r\n" + 
				"from\r\n" + 
				"(SELECT TO_HEX(SHA512(CONCAT(IFNULL(CAST(tft.corp as STRING),\"\"),IFNULL(CAST(tft.region as STRING),\"\"),IFNULL(CAST(tft.principal as STRING),\"\"),IFNULL(CAST(tft.associate as STRING),\"\"),IFNULL(CAST(tft.chain as STRING),\"\"),IFNULL(CAST(tft.source_id as STRING),\"\"),IFNULL(CAST(tft.file_date as STRING),\"\"),IFNULL(CAST(tft.file_time as STRING),\"\"),IFNULL(CAST(tft.merchant_number as STRING),\"\"),IFNULL(CAST(tft.cash_letter_number as STRING),\"\"),IFNULL(CAST(tft.card_acceptor_id as STRING),\"\"),IFNULL(CAST(tft.transaction_code as STRING),\"\"),IFNULL(CAST(tft.card_number_rk as STRING),\"\"),IFNULL(CAST(tft.transaction_amount as STRING),\"\"),IFNULL(CAST(tft.authorization_code as STRING),\"\"),IFNULL(CAST(tft.reference_number as STRING),\"\"),IFNULL(CAST(tft.transaction_date as STRING),\"\"),IFNULL(CAST(tft.transaction_time as STRING),\"\"),IFNULL(CAST(tft.transaction_id as STRING),\"\"),IFNULL(CAST(tft.authorization_amount as STRING),\"\"),IFNULL(CAST(tft.authorization_date as STRING),\"\"),IFNULL(CAST(tft.card_type as STRING),\"\"),IFNULL(CAST(tft.charge_type as STRING),\"\"),IFNULL(CAST(tft.merchant_dba_name as STRING),\"\"),IFNULL(CAST(tft.deposit_date as STRING),\"\"),IFNULL(CAST(tft.settled_amount as STRING),\"\"),IFNULL(CAST(tft.terminal_number as STRING),\"\"),IFNULL(CAST(tft.trans_id as STRING),\"\"),IFNULL(CAST(tft.trans_source as STRING),\"\")))) as la_trans_ft_sk, tft.*, 'MA' as source_code, dm_crd_typ.card_type_sk, dm_crd_typ.card_scheme, dm_crd_schm.card_scheme_sk ,dm_chrg_typ.charge_type_sk,dm_mcc.mcc_sk, dm_trns_cd. transaction_code_sk\r\n" + 
				"FROM\r\n" + 
				"  trusted_layer.la_trans_ft as tft \r\n" + 
				"LEFT JOIN \r\n" + 
				" ( SELECT distinct card_type,card_type_sk,card_scheme from transformed_layer.dim_card_type where current_ind=\"0\") dm_crd_typ\r\n" + 
				"on tft.card_type=dm_crd_typ.card_type\r\n" + 
				"LEFT JOIN \r\n" + 
				" ( SELECT distinct card_scheme,card_scheme_sk from transformed_layer.dim_card_scheme where current_ind=\"0\") dm_crd_schm\r\n" + 
				"on dm_crd_typ.card_scheme=dm_crd_schm.card_scheme\r\n" + 
				"LEFT JOIN \r\n" + 
				" ( SELECT distinct  charge_type_sk,charge_type,corporate from transformed_layer.dim_charge_type where current_ind=\"0\") dm_chrg_typ\r\n" + 
				"on tft.charge_type=dm_chrg_typ.charge_type and\r\n" + 
				"tft.corp=dm_chrg_typ.corporate \r\n" + 
				"LEFT JOIN \r\n" + 
				"( SELECT distinct mcc,mcc_sk  from `transformed_layer.dim_mcc`  where current_ind=\"0\") dm_mcc\r\n" + 
				"on dm_mcc.mcc=tft.merchant_category_code\r\n" + 
				"LEFT JOIN \r\n" + 
				"( SELECT distinct transaction_code, transaction_code_sk from `transformed_layer.dim_transaction_code`  where current_ind=\"0\") dm_trns_cd\r\n" + 
				"on dm_trns_cd.transaction_code=tft.transaction_code where tft.etlbatchid= '").append(batchId).append("' and tft.etl_batch_date= '").append(dateFromBatchId).append("' ) temptbl\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct authorization_source_code, card_scheme,authorization_source_code_desc from `transformed_layer.dim_authorization_src`) dm_auth_src\r\n" + 
						"on dm_auth_src.authorization_source_code=temptbl.authorization_source_code\r\n" + 
						"and dm_auth_src.card_scheme=temptbl.card_scheme\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct card_scheme_sk, avs_response_code, avs_response_code_sk from `transformed_layer.dim_avs_response_co`  where current_ind=\"0\") dm_avs_res_cd\r\n" + 
						"on dm_avs_res_cd.avs_response_code=temptbl.avs_response_code\r\n" + 
						"and temptbl.card_scheme_sk=dm_avs_res_cd.card_scheme_sk\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct cardholder_id_method_code, cardholder_id_method_desc,card_scheme from `transformed_layer.dim_cardholder_id_method` ) dm_crd_id_mtd\r\n" + 
						"on dm_crd_id_mtd.cardholder_id_method_code=temptbl.cardholder_id_method\r\n" + 
						"and temptbl.card_scheme=dm_crd_id_mtd.card_scheme\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct moto_ec_ind_short_desc,moto_ec_ind_long_desc,card_scheme,moto_ec_ind from `transformed_layer.dim_moto_ec_mp_1` ) dm_mt_rc_mp\r\n" + 
						"on dm_mt_rc_mp.moto_ec_ind=temptbl.moto_ec_indicator\r\n" + 
						"and dm_mt_rc_mp.card_scheme=temptbl.card_scheme\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct pos_entry_mode,pos_entry_mode_desc,pos_entry_mode_short_desc,card_scheme from  transformed_layer.dim_pos_entry_mode_by_card_ty  ) dm_pos_enty\r\n" + 
						"on dm_pos_enty.pos_entry_mode=temptbl.pos_entry_code\r\n" + 
						"and dm_pos_enty.card_scheme=temptbl.card_scheme\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct terminal_capability_code,card_scheme ,terminal_capability_code_desc from  transformed_layer.dim_term_capability_co  ) dm_trm_cap_code\r\n" + 
						"on dm_trm_cap_code.terminal_capability_code=temptbl.term_capability\r\n" + 
						"and dm_trm_cap_code.card_scheme=temptbl.card_scheme\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct visa_interchange_level ,visa_interchange_level_desc from  transformed_layer.dim_visa_intercha ) dm_visa_itrchng\r\n" + 
						"on dm_visa_itrchng.visa_interchange_level=temptbl.visa_interchange_level\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct  mc_interchange_level  ,mc_interchange_level_desc from  transformed_layer.dim_mc_interchan ) dm_mc_itrchng\r\n" + 
						"on dm_mc_itrchng.mc_interchange_level =temptbl.mc_interchange_level \r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct  mc_device_type_code_desc,mc_device_type_code  from  transformed_layer.dim_mc_device_type ) dm_mc_dvc_typ\r\n" + 
						"on dm_mc_dvc_typ.mc_device_type_code  = temptbl.mc_device_type_cd\r\n" + 
						"\r\n" + 
						"LEFT JOIN \r\n" + 
						"( SELECT distinct source_sk,	source_code from  transformed_layer.dim_source where source_code ='MA') dm_src\r\n" + 
						"on dm_src.source_code  = temptbl.source_code").toString();
	}
}
