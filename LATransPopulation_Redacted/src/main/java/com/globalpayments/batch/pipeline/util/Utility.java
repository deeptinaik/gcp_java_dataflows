package com.globalpayments.batch.pipeline.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class Utility  implements CommonUtil
{
	/**
	 * <p>
	 * <B>Description : </B> Fetching table data from bigQuery tables.
	 */
	public static PCollection<KV<String, TableRow>> fetchTableRowsUsingQuery(PBegin pBegin, String fetchingQuery,
			String stringComment, String[] key)
	{
		return fetchTableRowsUsingQuery(pBegin, fetchingQuery, stringComment, new ReadKvData(key));
	}

	public static PCollection<KV<String, TableRow>> fetchTableRowsUsingQuery(PBegin pBegin, String fetchingQuery,
			String stringComment, SerializableFunction<SchemaAndRecord, KV<String, TableRow>> readFunction)
	{
		return pBegin.apply(stringComment,
				BigQueryIO.read(readFunction).fromQuery(fetchingQuery).withTemplateCompatibility().withoutValidation().usingStandardSql()
						.withCoder(KvCoder.<String, TableRow>of(StringUtf8Coder.of(), TableRowJsonCoder.of())));
	}
	

	public static PCollection<TableRow> fetchTableRows(PBegin pBegin, String tableName, String stringComment) {
		return pBegin.apply(stringComment,
				BigQueryIO.readTableRows().from(tableName).withTemplateCompatibility().withoutValidation());
	}
	
	
	
	
	public static PCollection<KV<String, TableRow>> fetchTableRows(PBegin pBegin, String tableName,
			String stringComment, String[] columnList, String key[])
	{
		return pBegin.apply(stringComment,
				BigQueryIO.read(new ReadKvData(key)).from(tableName).withMethod(Method.DIRECT_READ)
						.withSelectedFields(Arrays.asList(columnList)).withTemplateCompatibility().withoutValidation()
						.withCoder(KvCoder.<String, TableRow>of(StringUtf8Coder.of(), TableRowJsonCoder.of())));
	}

	
	
	public static Map<String, String> getTrustedTransSchema() {

		Map<String, String> tableFieldMapping = new HashMap<>();

		// tableFieldMapping.put(Transformed_layer_Field, Trusted_layer_Field);
		tableFieldMapping.put(ETL_BATCH_DATE, ETL_BATCH_DATE);
		tableFieldMapping.put(HIERARCHY, new StringBuilder(CORP).append(",").append(REGION).append(",")
				.append(PRINCIPAL).append(",").append(ASSOCIATE).append(",").append(CHAIN).toString());
		tableFieldMapping.put(CORPORATE, CORP);
		tableFieldMapping.put(REGION, REGION);
		tableFieldMapping.put(PRINCIPAL, PRINCIPAL);
		tableFieldMapping.put(ASSOCIATE, ASSOCIATE);
		tableFieldMapping.put(CHAIN, CHAIN);
		tableFieldMapping.put(MERCHANT_NUMBER, MERCHANT_NUMBER);
		tableFieldMapping.put(MERCHANT_NUMBER_INT, MERCHANT_NUMBER);
		tableFieldMapping.put(TRANSACTION_DATE, TRANSACTION_DATE);
		tableFieldMapping.put(TRANSACTION_TIME, TRANSACTION_TIME);
		tableFieldMapping.put(TRANSACTION_IDENTIFIER, TRANSACTION_ID);
		tableFieldMapping.put(AVS_RESPONSE_CODE, AVS_RESPONSE_CODE);
		tableFieldMapping.put(CARD_TYPE, CARD_TYPE);
      	tableFieldMapping.put(CHARGE_TYPE, CHARGE_TYPE);
		tableFieldMapping.put(MCC, MERCHANT_CATEGORY_CODE);
		tableFieldMapping.put(TRANSACTION_CODE, TRANSACTION_CODE);
		tableFieldMapping.put(TRANSACTION_CURRENCY_CODE, TRANSACTION_CURRENCY_CODE_NEW);
		tableFieldMapping.put(SETTLE_CURRENCY_CODE, SETTLED_CURRENCY_CODE);
		tableFieldMapping.put(SETTLED_AMOUNT, SETTLED_AMOUNT);
		tableFieldMapping.put(AUTHORIZATION_CHARACTERISTIC_IND, ACI);
		tableFieldMapping.put(AUTHORIZATION_AMOUNT, AUTHORIZATION_AMOUNT);
		tableFieldMapping.put(AUTHORIZATION_CODE, AUTHORIZATION_CODE);
		tableFieldMapping.put(AUTHORIZATION_DATE, AUTHORIZATION_DATE);
		tableFieldMapping.put(AUTHORIZATION_SOURCE_CODE, AUTHORIZATION_SOURCE_CODE);
		tableFieldMapping.put(AUTHORIZATION_RESPONSE, AUTHORIZATION_RESPONSE);
		tableFieldMapping.put(LA_TRANS_FT_SK, LA_TRANS_FT_SK);
		
		tableFieldMapping.put(CAR_RENTAL_CHECKOUT_DATE, CAR_RENTAL_CHECK_OUT_DATE);
		tableFieldMapping.put(CAR_RENTAL_EXTRA_CHARGES_CODE, CAR_RENTAL_EXTRA_CHARGES);
		tableFieldMapping.put(CAR_RENTAL_NO_SHOW_CODE, CAR_RENTAL_NO_SHOW);
		tableFieldMapping.put(CARD_ACCEPTOR_IDENTIFIER, CARD_ACCEPTOR_ID);
		tableFieldMapping.put(CARDHOLDER_ACTIVATED_TERMINAL_IND, CARDHOLDER_ACTIVATED_TERM);
		tableFieldMapping.put(CARDHOLDER_ID_METHOD, CARDHOLDER_ID_METHOD);
		tableFieldMapping.put(CASH_LETTER_NUMBER, CASH_LETTER_NUMBER);
		tableFieldMapping.put(CENTRAL_TIME_IND, CENTRAL_TIME_INDICATOR);
		tableFieldMapping.put(CUSTOM_PAYMENT_SERVICE_IND, CPS_INDICATOR);
		tableFieldMapping.put(CROSS_BORDER_CODE, CROSS_BORDER_INDICATOR);
		tableFieldMapping.put(DEPOSIT_DATE, DEPOSIT_DATE);
		tableFieldMapping.put(FILE_DATE, DEPOSIT_DATE);
		tableFieldMapping.put(DEPOSIT_IDENTIFIER, DEPOSIT_ID);
		tableFieldMapping.put(FILE_DATE_ORIGINAL, FILE_DATE);
		tableFieldMapping.put(FILE_TIME, FILE_TIME);
		tableFieldMapping.put(LODGING_CHECKIN_DATE, LODGING_CHECK_IN_DATE);
		tableFieldMapping.put(LODGING_EXTRA_CHARGE_CODE, LODGING_EXTRA_CHARGES);
		tableFieldMapping.put(LODGING_NO_SHOW_IND, LODGING_NO_SHOW_INDICATOR);
		tableFieldMapping.put(MARKET_SPECIFIC_AUTH_DATA_CODE, MARKET_SPECIFIC_AUTH_DATA);
		tableFieldMapping.put(MERCHANT_NAME, MERCHANT_DBA_NAME);
		tableFieldMapping.put(MOTO_EC_IND, MOTO_EC_INDICATOR);
		tableFieldMapping.put(POS_ENTRY_CODE, POS_ENTRY_CODE);
		tableFieldMapping.put(PREPAID_CODE, PREPAID_INDICATOR);
		tableFieldMapping.put(PURCHASE_IDENTIFIER, PURCHASE_IDENTIFIER);
		tableFieldMapping.put(PURCHASE_IDENTIFIER_FORMAT, PURCHASE_IDENTIFIER_FORMAT);
		tableFieldMapping.put(REF_NUMBER, REFERENCE_NUMBER);
		tableFieldMapping.put(REQUESTED_PAYMENT_SERVICE_VALUE, REQ_PAYMENT_SERVICE_VALUE);
		tableFieldMapping.put(RESPONSE_DOWNGRADE_CODE, RESPONSE_DOWNGRADE_CODE);
		tableFieldMapping.put(SCAN_CHARGE_FILE_IDENTIFIER, SCAN_CHARGE);
		tableFieldMapping.put(SERVICE_DEVELOPMENT_FIELD, SERVICE_DEVELOPMENT_FIELD);

		tableFieldMapping.put(SOURCE_IDENTIFIER, SOURCE_ID);
		tableFieldMapping.put(SUPPLEMENTAL_AUTHORIZATION_AMOUNT, SUP_AUTHORIZATION_AMOUNT);
		tableFieldMapping.put(TERMINAL_CAPABILITY, TERM_CAPABILITY);
		tableFieldMapping.put(ERROR_CODE, ERROR_CODE);
		tableFieldMapping.put(TRANSACTION_AMOUNT, TRANSACTION_AMOUNT_NEW);
		tableFieldMapping.put(VALIDATION_CODE, VALIDATION_CODE);
		tableFieldMapping.put(VISA_INTERCHANGE_LEVEL, VISA_INTERCHANGE_LEVEL);
		tableFieldMapping.put(VISA_PRODUCT_LEVEL_IDENTIFIER, VISA_PRODUCT_LEVEL_ID);
		tableFieldMapping.put(MC_INTERCHANGE_LEVEL, MC_INTERCHANGE_LEVEL);
		tableFieldMapping.put(MC_DEVICE_TYPE_CODE, MC_DEVICE_TYPE_CD);
		
		
		//Needs to check
		tableFieldMapping.put(MC_UCAF_CODE,MC_UCAF);
		tableFieldMapping.put(ALL_SOURCE_TERMINAL_ID, TERMINAL_NUMBER);
		
		tableFieldMapping.put(SUB_MERCHANT_IDENTIFIER,SUB_MERCHANT_ID);
		tableFieldMapping.put(GLOBAL_TRAN_ID,TRANS_ID);
		tableFieldMapping.put(GLOBAL_TRAN_ID_SOURCE,TRANS_SOURCE);
		
		tableFieldMapping.put(CARD_NUMBER_SK, CARD_NUMBER_SK);
		tableFieldMapping.put(CARD_NUMBER_RK, CARD_NUMBER_RK);
		tableFieldMapping.put(MERCHANT_TOKEN, MERCHANT_TOKEN);
		tableFieldMapping.put(GLOBAL_TOKEN, GLOBAL_TOKEN);

		tableFieldMapping.put(BATCH_CONTROL_NUMBER,
				new StringBuilder(DEPOSIT_DATE).append(",").append(CASH_LETTER_NUMBER).toString());
		tableFieldMapping.put(ETLBATCHID, ETLBATCHID);
		tableFieldMapping.put(SEQ_NUMBER_TOKEN,SEQUENCE_KEY);

		tableFieldMapping.put(CORPORATE_SK,CORPORATE_SK);
		tableFieldMapping.put(REGION_SK,REGION_SK);
		tableFieldMapping.put(PRINCIPAL_SK,PRINCIPAL_SK);
		tableFieldMapping.put(ASSOCIATE_SK,ASSOCIATE_SK);
		tableFieldMapping.put(CHAIN_SK,CHAIN_SK);
		tableFieldMapping.put(SOURCE_SK,SOURCE_SK);
		tableFieldMapping.put(SOURCE_CODE,SOURCE_CODE);
		tableFieldMapping.put(AVS_RESPONSE_CODE_SK,AVS_RESPONSE_CODE_SK);
		tableFieldMapping.put(CARD_TYPE_SK,CARD_TYPE_SK);
		tableFieldMapping.put(CARD_SCHEME_SK,CARD_SCHEME_SK);
		tableFieldMapping.put(CARD_SCHEME,CARD_SCHEME);
		tableFieldMapping.put(CHARGE_TYPE_SK,CHARGE_TYPE_SK);
		tableFieldMapping.put(MCC_SK,MCC_SK);
		tableFieldMapping.put(TRANSACTION_CODE_SK,TRANSACTION_CODE_SK);
		tableFieldMapping.put(AUTHORIZATION_SOURCE_CODE_DESC,AUTHORIZATION_SOURCE_CODE_DESC);
		tableFieldMapping.put(CARDHOLDER_ID_METHOD_DESC,CARDHOLDER_ID_METHOD_DESC);
		tableFieldMapping.put(MOTO_EC_IND_SHORT_DESC,MOTO_EC_IND_SHORT_DESC);
		tableFieldMapping.put(MOTO_EC_IND_LONG_DESC,MOTO_EC_IND_LONG_DESC);
		tableFieldMapping.put(POS_ENTRY_CODE_DESC,POS_ENTRY_CODE_DESC);
		tableFieldMapping.put(POS_ENTRY_MODE_SHORT_DESC, POS_ENTRY_MODE_SHORT_DESC);
		tableFieldMapping.put(TERMINAL_CAPABILITY_CODE_DESC,TERMINAL_CAPABILITY_CODE_DESC);
		tableFieldMapping.put(VISA_INTERCHANGE_LEVEL_DESC,VISA_INTERCHANGE_LEVEL_DESC);
		tableFieldMapping.put(MC_INTERCHANGE_LEVEL_DESC,MC_INTERCHANGE_LEVEL_DESC);
		tableFieldMapping.put(MC_DEVICE_TYPE_CODE_DESC,MC_DEVICE_TYPE_CODE_DESC);
	
		
		
		return tableFieldMapping;
	}

	
	
	public static Boolean isValidDate(String dateForValidation) {
		try {
			DateFormat df = new SimpleDateFormat(DATE_FORMAT_YYYYMMDD);
			df.setLenient(false);
			df.parse(dateForValidation);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}
	
	

	public static String getValidTime(Object timeObject) {
		try {
			if (null != timeObject && 6 == timeObject.toString().trim().length()) {
				String timeString = timeObject.toString();
				StringBuilder timeFormatBuilder = new StringBuilder(timeString.substring(0, 2)).append(":")
						.append(timeString.substring(2, 4)).append(":").append(timeString.substring(4, 6));

				DateFormat df = new SimpleDateFormat("HH:mm:ss");
				df.setLenient(false);
				df.parse(timeFormatBuilder.toString());
				return timeFormatBuilder.toString();
			} else {
				return null;
			}
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static String trimLeadingZeros(String inputString) {
		if (null == inputString)
			return inputString;

		return inputString.replaceFirst("^0+(?!$)", "");
	}
	
	public static String parseDate(Object value, String inputDateformat) throws ParseException {
		if (null != value && !value.toString().isEmpty()) {
			String dateFormat = inputDateformat;
			DateFormat format = new SimpleDateFormat(dateFormat);
			format.setLenient(false);
			Date date = format.parse(value.toString());
			return new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD).format(date);
		} else {
			return BLANK;
		}
	}
	
	
	public static PCollectionView<Map<String, TableRow>> convertTableRowInToMap(
			PCollection<KV<String, TableRow>> sourceTableData)
	{

		PCollection<KV<String, TableRow>> sourceDataKV = sourceTableData
				.apply(Distinct.withRepresentativeValueFn(new UniqueKeyFunction()))
				.setCoder(KvCoder.<String, TableRow>of(StringUtf8Coder.of(), TableRowJsonCoder.of()));

		return sourceDataKV.apply("create view", View.asMap());
	}

	public static List<String> getDimMerchantInfoRequiredColumnList()
	{
		List<String> columnList = new ArrayList<>();
		
		
		columnList.add("dmi_merchant_number");
		columnList.add("dmi_corporate");
		columnList.add("dmi_region");
		
		return columnList;
	}
	
	public static List<String> getDefaultCurrenciesLkpColumnList()
	{
		List<String> columnList = new ArrayList<>();
		
		columnList.add("ddc_corporate_region");
		columnList.add("ddc_currency_code");
		
		return columnList;
	}
	
	public static List<String> getDimISoNumericCurrencyCodeColumnList()
	{
		List<String> columnList = new ArrayList<>();
		
		columnList.add("dincc_currency_code");
		columnList.add("dincc_iso_numeric_currency_code");
		columnList.add("dincc_iso_numeric_currency_code_sk");
		return columnList;
	}
	
	
	public static Map<String, String> getTransFT1Fields() 
	{

		Map<String, String> tableFieldMapping = getTransFT0Fields();

		tableFieldMapping.put(SOURCE_CODE, SOURCE_CODE);
		tableFieldMapping.put(SOURCE_SK, SOURCE_SK);
		tableFieldMapping.put(CARD_TYPE_SK, CARD_TYPE_SK);
		tableFieldMapping.put(CARD_SCHEME, CARD_SCHEME);
		tableFieldMapping.put(CARD_SCHEME_SK, CARD_SCHEME_SK);
		tableFieldMapping.put(CHARGE_TYPE_SK, CHARGE_TYPE_SK);
		tableFieldMapping.put(MCC_SK, MCC_SK);
		tableFieldMapping.put(MERCHANT_INFORMATION_SK, MERCHANT_INFORMATION_SK);

		return tableFieldMapping;
	}
	
	
	
	public static Map<String, String> getTransFT0Fields() {

		Map<String, String> tableFieldMapping = getTrustedTransSchema();

		tableFieldMapping.put(CORPORATE_SK, CORPORATE_SK);
		tableFieldMapping.put(REGION_SK, REGION_SK);
		tableFieldMapping.put(PRINCIPAL_SK, PRINCIPAL_SK);
		tableFieldMapping.put(ASSOCIATE_SK, ASSOCIATE_SK);
		tableFieldMapping.put(CHAIN_SK, CHAIN_SK);

		return tableFieldMapping;
	}
	
	public static Map<String, String> finalFields() 
	{

		Map<String, String> tableFieldMapping = getTransFT2Fields();

		tableFieldMapping.put(AVS_RESPONSE_CODE_SK, AVS_RESPONSE_CODE_SK);
		tableFieldMapping.put(TRANSACTION_CURRENCY_CODE_SK, TRANSACTION_CURRENCY_CODE_SK);
		tableFieldMapping.put(LA_TRANS_FT_UUID, LA_TRANS_FT_UUID);
		tableFieldMapping.put(MERCHANT_NUMBER_INT, MERCHANT_NUMBER_INT);
		tableFieldMapping.put(SETTLE_ALPHA_CURRENCY_CODE, SETTLE_ALPHA_CURRENCY_CODE);
		tableFieldMapping.put(SETTLE_ISO_CURRENCY_CODE, SETTLE_ISO_CURRENCY_CODE);

		// dim table fields mapping

		tableFieldMapping.put(AUTHORIZATION_SOURCE_CODE_DESC, AUTHORIZATION_SOURCE_CODE_DESC);
		tableFieldMapping.put(CARDHOLDER_ID_METHOD_DESC, CARDHOLDER_ID_METHOD_DESC);
		tableFieldMapping.put(MOTO_EC_IND_SHORT_DESC, MOTO_EC_IND_SHORT_DESC);
		tableFieldMapping.put(MOTO_EC_IND_LONG_DESC, MOTO_EC_IND_LONG_DESC);
		tableFieldMapping.put(POS_ENTRY_CODE_DESC, POS_ENTRY_CODE_DESC);
		tableFieldMapping.put(POS_ENTRY_MODE_SHORT_DESC, POS_ENTRY_MODE_SHORT_DESC);
		tableFieldMapping.put(TERMINAL_CAPABILITY_CODE_DESC, TERMINAL_CAPABILITY_CODE_DESC);
		tableFieldMapping.put(VISA_INTERCHANGE_LEVEL_DESC, VISA_INTERCHANGE_LEVEL_DESC);
		tableFieldMapping.put(MC_INTERCHANGE_LEVEL_DESC, MC_INTERCHANGE_LEVEL_DESC);
		tableFieldMapping.put(MC_DEVICE_TYPE_CODE_DESC, MC_DEVICE_TYPE_CODE_DESC);

		return tableFieldMapping;
	}
	
	public static Map<String, String> getTransFT2Fields() 
	{

		Map<String, String> tableFieldMapping = getTransFT1Fields();

		tableFieldMapping.put(ALPHA_CURRENCY_CODE, ALPHA_CURRENCY_CODE);
		tableFieldMapping.put(ISO_NUMERIC_CURRENCY_CODE, ISO_NUMERIC_CURRENCY_CODE);
		return tableFieldMapping;
	}
	
}




