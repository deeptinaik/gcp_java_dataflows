package com.globalpayments.batch.pipeline.util;

public interface CommonUtil {
	
	public static final String NULL = "null";
	public static final String BLANK = "";
	public static final String DOT=".";
	public static final String DATE_FORMAT_YYYYMMDD = "yyyyMMdd";
	public static final String DATE_FORMAT_MMDDYY = "MMddyy z";
	public static final String DATE_FORMAT_YYMMDD = "yyMMdd z";
	public static final String DATE_FORMAT_MMDD = "MMdd z";
	public static final String DATE_FORMAT_YYYYMMDDZ = "yyyyMMdd z";
	public static final String DATE_FORMAT_MMYY="MMyy";
	public static final String DATE_FORMAT_YYYY_MM_DD="yyyy-MM-dd";
	public static final String DATE_FORMAT_YYYY_MM_DD_TIME="yyyy-MM-dd HH:mm:ss";
	
	
	public static final String TRANSFT_INTAKE_TABLE = "trusted_layer.la_trans_ft";
	public static final String LA_TRANS_FT_SK = "la_trans_ft_sk";
	
	//TransFT Source field name
	public static final String ETLBATCHID="etlbatchid";
	public static final String CREATE_DATE_TIME = "create_date_time";
	public static final String UPDATE_DATE_TIME = "update_date_time";
	
	public static final String MERCHANT_NUMBER = "merchant_number";
	public static final String MERCHANT_NUMBER_INT = "merchant_number_int";
	public static final String HIERARCHY = "hierarchy";
	public static final String CORPORATE = "corporate";
	public static final String REGION = "region";
	public static final String MERCHANT_INFORMATION_SK = "merchant_information_sk";
	
	public static final String ISO_NUMERIC_CURRENCY_CODE = "iso_numeric_currency_code";
	public static final String CURRENCY_CODE = "currency_code";
	public static final String ISO_NUMERIC_CURRENCY_CODE_SK = "iso_numeric_currency_code_sk";
	
	public static final String TRANSACTION_NUMERIC_CURRENCY_CODE_SK = "transaction_currency_code_sk";

	public static final String AVS_RESPONSE_CODE = "avs_response_code";
	public static final String CARD_SCHEME_SK = "card_scheme_sk";
	public static final String AVS_RESPONSE_CODE_SK = "avs_response_code_sk";

	public static final String DIM_MERCHANT_INFORMATION_QUERY = "Select  merchant_number, hierarchy, corporate, region, merchant_information_sk from transformed_layer.dim_merchant_info where current_ind='0'";
	public static final String dim_iso_numeric_curr_code = "transformed_layer.dim_iso_numeric_curr_code";
	public static final String CURRENT_IND = "current_ind";
	public static final String ZERO = "0";
	public static final String dim_avs_response_co = "transformed_layer.dim_avs_response_co";
	public static final String LA_TRANS_FT = "transformed_layer.la_trans_ft";
	
	
	// TransFT Source field name
		public static final String SEQ_NUMBER_TOKEN = "seq_number_token";
		public static final String SEQUENCE_KEY = "sequence_key";
		public static final String CORP = "corp";

		public static final String PRINCIPAL = "principal";
		public static final String ASSOCIATE = "associate";
		public static final String CHAIN = "chain";
		public static final String TRANSACTION_DATE = "transaction_date";
		public static final String TRANSACTION_TIME = "transaction_time";
		public static final String TRANSACTION_ID = "transaction_id";
		public static final String TRANSACTION_IDENTIFIER = "transaction_identifier";
		
		public static final String CORPORATE_SK = "corporate_sk";
		public static final String CARD_TYPE = "card_type";
		public static final String CHARGE_TYPE = "charge_type";	
		public static final String MCC = "mcc";
		public static final String MERCHANT_CATEGORY_CODE = "merchant_category_code";
		public static final String TRANSACTION_CODE = "transaction_code";
		
		public static final String TRANSACTION_CURRENCY_CODE_NEW = "transaction_currency_code_new";
		public static final String SETTLED_CURRENCY_CODE = "settled_currency_code";
		public static final String SETTLE_CURRENCY_CODE = "settle_currency_code";
		public static final String SETTLE_AMOUNT = "settle_amount";
		public static final String AUTHORIZATION_CHARACTERISTIC_IND = "authorization_characteristic_ind";
		public static final String ACI = "aci";
		public static final String AUTHORIZATION_AMOUNT = "authorization_amount";
		public static final String AUTHORIZATION_CODE = "authorization_code";
		public static final String AUTHORIZATION_DATE = "authorization_date";
		public static final String AUTHORIZATION_SOURCE_CODE = "authorization_source_code";
		public static final String AUTHORIZATION_RESPONSE = "authorization_response";
		
		public static final String CAR_RENTAL_CHECKOUT_DATE = "car_rental_checkout_date";
		public static final String CAR_RENTAL_CHECK_OUT_DATE = "car_rental_check_out_date";
		public static final String CAR_RENTAL_EXTRA_CHARGES = "car_rental_extra_charges";
		public static final String CAR_RENTAL_EXTRA_CHARGES_CODE = "car_rental_extra_charges_code";
		public static final String CAR_RENTAL_NO_SHOW = "car_rental_no_show";
		public static final String CAR_RENTAL_NO_SHOW_CODE = "car_rental_no_show_code";
		
		public static final String CARD_ACCEPTOR_IDENTIFIER = "card_acceptor_identifier";
		public static final String CARD_ACCEPTOR_ID = "card_acceptor_id";
	
		public static final String CARDHOLDER_ACTIVATED_TERM = "cardholder_activated_term";
		public static final String CARDHOLDER_ACTIVATED_TERMINAL_IND = "cardholder_activated_terminal_ind";
		public static final String CARDHOLDER_ID_METHOD = "cardholder_id_method";
		public static final String CASH_LETTER_NUMBER = "cash_letter_number";
		public static final String CENTRAL_TIME_INDICATOR = "central_time_indicator";
		public static final String CENTRAL_TIME_IND = "central_time_ind";
		public static final String CUSTOM_PAYMENT_SERVICE_IND = "custom_payment_service_ind";
		public static final String CPS_INDICATOR = "cps_indicator";
		public static final String CROSS_BORDER_INDICATOR = "cross_border_indicator";
		public static final String CROSS_BORDER_CODE = "cross_border_code";
		
		public static final String DEPOSIT_DATE = "deposit_date";
		public static final String FILE_DATE = "file_date";
		public static final String DEPOSIT_IDENTIFIER = "deposit_identifier";
		public static final String DEPOSIT_ID = "deposit_id";
		
		public static final String FILE_DATE_ORIGINAL = "file_date_original";
		public static final String FILE_TIME = "file_time";
		
		public static final String SETTLED_AMOUNT = "settled_amount";
		public static final String LODGING_CHECKIN_DATE = "lodging_checkin_date";
		public static final String LODGING_CHECK_IN_DATE = "lodging_check_in_date";
		
		
		public static final String LODGING_EXTRA_CHARGE_CODE = "lodging_extra_charge_code";
		public static final String LODGING_EXTRA_CHARGES = "lodging_extra_charges";
		public static final String LODGING_NO_SHOW_IND = "lodging_no_show_ind";
		public static final String LODGING_NO_SHOW_INDICATOR = "lodging_no_show_indicator";
		public static final String MARKET_SPECIFIC_AUTH_DATA_CODE = "market_specific_auth_data_code";
		public static final String MARKET_SPECIFIC_AUTH_DATA = "market_specific_auth_data";
		public static final String MERCHANT_NAME = "merchant_name";
		public static final String MERCHANT_DBA_NAME = "merchant_dba_name";
		public static final String MOTO_EC_INDICATOR = "moto_ec_indicator";
		public static final String MOTO_EC_IND = "moto_ec_ind";
		public static final String POS_ENTRY_CODE = "pos_entry_code";
		public static final String PREPAID_CODE = "prepaid_code";
		public static final String PREPAID_INDICATOR = "prepaid_indicator";
		public static final String PURCHASE_IDENTIFIER = "purchase_identifier";
		public static final String PURCHASE_IDENTIFIER_FORMAT = "purchase_identifier_format";
		
		public static final String REF_NUMBER = "ref_number";
		public static final String REFERENCE_NUMBER = "reference_number";
		public static final String REQUESTED_PAYMENT_SERVICE_VALUE = "requested_payment_service_value";
		public static final String REQ_PAYMENT_SERVICE_VALUE = "req_payment_service_value";
		public static final String RESPONSE_DOWNGRADE_CODE = "response_downgrade_code";
		public static final String SCAN_CHARGE_FILE_IDENTIFIER = "scan_charge_file_identifier";
		public static final String SCAN_CHARGE = "scan_charge";
		public static final String SERVICE_DEVELOPMENT_FIELD = "service_development_field";
		
		public static final String SOURCE_IDENTIFIER = "source_identifier";
		public static final String SOURCE_ID = "source_id";
		
		
		public static final String SUPPLEMENTAL_AUTHORIZATION_AMOUNT = "supplemental_authorization_amount";
		public static final String SUP_AUTHORIZATION_AMOUNT = "sup_authorization_amount";
		
		public static final String TERM_CAPABILITY = "term_capability";
		public static final String TERMINAL_CAPABILITY= "terminal_capability";
		//public static final String TERMINAL_CAPABILITY_CODE = "terminal_capability_code";
		
		public static final String ERROR_CODE = "error_code";
		
		public static final String TRANSACTION_AMOUNT_NEW = "transaction_amount_new";
		public static final String TRANSACTION_AMOUNT = "transaction_amount";
		public static final String VALIDATION_CODE = "validation_code";
		public static final String VISA_INTERCHANGE_LEVEL = "visa_interchange_level";
		
		public static final String VISA_PRODUCT_LEVEL_IDENTIFIER = "visa_product_level_identifier";
		public static final String VISA_PRODUCT_LEVEL_ID="visa_product_level_id";
		public static final String MC_INTERCHANGE_LEVEL = "mc_interchange_level";
		public static final String MC_DEVICE_TYPE_CODE = "mc_device_type_code";
		
		public static final String MC_DEVICE_TYPE_CD = "mc_device_type_cd";
		
		public static final String MC_UCAF_CODE="mc_ucaf_code";
		public static final String MC_UCAF="mc_ucaf";
		
		public static final String ALL_SOURCE_TERMINAL_ID = "all_source_terminal_id";
		//public static final String TERMINAL_ID = "terminal_id";
		public static final String TERMINAL_NUMBER = "terminal_number";
		public static final String SUB_MERCHANT_ID="sub_merchant_id";
		public static final String TRANS_ID="trans_id";
		public static final String TRANS_SOURCE="trans_source";
		
		public static final String SUB_MERCHANT_IDENTIFIER= "sub_merchant_identifier";
		public static final String GLOBAL_TRAN_ID= "global_tran_id";
		public static final String GLOBAL_TRAN_ID_SOURCE= "global_tran_id_source";

		public static final String CARD_NUMBER_SK = "card_number_sk";
		public static final String CARD_NUMBER_RK = "card_number_rk";
		public static final String MERCHANT_TOKEN = "merchant_token";
		public static final String GLOBAL_TOKEN = "global_token";
		
		public static final String BATCH_CONTROL_NUMBER = "batch_control_number";
		public static final String TRANSACTION_CURRENCY_CODE = "transaction_currency_code";
		public static final String ETL_BATCH_DATE="etl_batch_date";
		
		
		public static final String MHD_CORP = "MHD_corp";
		public static final String MHD_CORPORATE = "MHD_corporate";
		public static final String MHD_REGION = "MHD_region";
		public static final String MHD_HIERARCHY = "MHD_hierarchy";
		
		
		// TempTransFT table field names
		public static final String CORP_SK = "corp_sk";
		public static final String REGION_SK = "region_sk";
		public static final String PRINCIPAL_SK = "principal_sk";
		public static final String ASSOCIATE_SK = "associate_sk";
		public static final String CHAIN_SK = "chain_sk";

		
		public static final String DIM_CORPORATE = "transformed_layer.dim_corporate";
		public static final String DIM_REGION = "transformed_layer.dim_region";
		public static final String DIM_PRINCIPAL = "transformed_layer.dim_principal";
		public static final String DIM_ASSOCIATE = "transformed_layer.dim_associate";
		public static final String DIM_CHAIN = "transformed_layer.dim_chain";
		
		// TransFT constant variable
		public static final String DMX_LOOKUP_FAILURE = "-2";
		public static final String DMX_LOOKUP_NULL_BLANK = "-1";
		
		public static final String SOURCE_CODE = "source_code";
		public static final String SOURCE_SK = "source_sk";
		public static final String TRANS_SK = "trans_sk"; // TODO check
		public static final String SOURCE_HOME_MA = "MA";
		
		// CardTypeDm fields
		public static final String CARD_TYPE_SK = "card_type_sk";
		public static final String CARD_SCHEME = "card_scheme";
		public static final String CHARGE_TYPE_SK = "charge_type_sk";
		public static final String MCC_SK = "mcc_sk";
		public static final String TRANSACTION_CODE_SK = "transaction_code_sk";
		public static final String AUTHORIZATION_SOURCE_CODE_DESC = "authorization_source_code_desc";
		public static final String CARDHOLDER_ID_METHOD_DESC = "cardholder_id_method_desc";
		public static final String MOTO_EC_IND_SHORT_DESC = "moto_ec_ind_short_desc";
		public static final String MOTO_EC_IND_LONG_DESC = "moto_ec_ind_long_desc";
		public static final String POS_ENTRY_CODE_DESC = "pos_entry_code_desc";
		public static final String POS_ENTRY_MODE_SHORT_DESC = "pos_entry_mode_short_desc";
		public static final String TERMINAL_CAPABILITY_CODE_DESC = "terminal_capability_code_desc";
		public static final String VISA_INTERCHANGE_LEVEL_DESC = "visa_interchange_level_desc";
		public static final String MC_INTERCHANGE_LEVEL_DESC = "mc_interchange_level_desc";
		public static final String MC_DEVICE_TYPE_CODE_DESC = "mc_device_type_code_desc";
		
		
		public static final String DIM_DEFAULT_CURRENCY_QUERY = "select distinct  concat(cast(corporate as string),cast(region as string)) as ddc_corporate_region, "
				+ "currency_code as ddc_currency_code from `transformed_layer.dim_default_curr`";
		public static final String DIM_ISO_NUMERIC_CURRENCY_CODE_QUERY = "select distinct currency_code as dincc_currency_code,iso_numeric_currency_code"
				+ " as dincc_iso_numeric_currency_code ,iso_numeric_currency_code_sk as dincc_iso_numeric_currency_code_sk from `transformed_layer.dim_iso_numeric_curr_code`";
		
	
		
		public static final String ALPHA_CURRENCY_CODE = "alpha_currency_code";
		public static final String SETTLE_ALPHA_CURRENCY_CODE = "settle_alpha_currency_code";

		public static final String SETTLE_ISO_CURRENCY_CODE = "settle_iso_currency_code";
		
		public static final String TRANSACTION_CURRENCY_CODE_SK = "transaction_currency_code_sk";
		
		
		// added column changes for hash key implementation
		public static final String LA_TRANS_FT_UUID="la_trans_ft_uuid";
		public static final String[] COLUMNS={"corp","region","principal","associate","chain","source_id","file_date","file_time","merchant_number","cash_letter_number","card_acceptor_id","transaction_code","card_number_rk","transaction_amount","authorization_code","reference_number","transaction_date","transaction_time","transaction_id","authorization_amount","authorization_date","card_type","charge_type","merchant_dba_name","deposit_date","settled_amount","terminal_number","trans_id","trans_source"};
}
