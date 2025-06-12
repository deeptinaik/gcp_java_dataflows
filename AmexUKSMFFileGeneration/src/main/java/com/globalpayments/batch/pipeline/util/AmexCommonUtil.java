package com.globalpayments.batch.pipeline.util;

/**
 * Common utility interface for Amex UK SMF File Generation Pipeline
 * Contains constants, table names, and field mappings used throughout the pipeline
 */
public interface AmexCommonUtil {
    
    // Date formats
    public static final String DATE_FORMAT_YYYYMMDD = "yyyyMMdd";
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String DATE_FORMAT_MMDDYY = "MMddyy";
    
    // Common field names
    public static final String TRANSACTION_ID = "transaction_id";
    public static final String MERCHANT_NUMBER = "merchant_number";
    public static final String MERCHANT_NAME = "merchant_name";
    public static final String TRANSACTION_DATE = "transaction_date";
    public static final String TRANSACTION_TIME = "transaction_time";
    public static final String TRANSACTION_AMOUNT = "transaction_amount";
    public static final String CURRENCY_CODE = "currency_code";
    public static final String SETTLEMENT_AMOUNT = "settlement_amount";
    public static final String SETTLEMENT_CURRENCY = "settlement_currency";
    public static final String CARD_NUMBER = "card_number";
    public static final String CARD_TYPE = "card_type";
    public static final String AUTHORIZATION_CODE = "authorization_code";
    public static final String BATCH_NUMBER = "batch_number";
    public static final String TERMINAL_ID = "terminal_id";
    public static final String ETL_BATCH_DATE = "etl_batch_date";
    public static final String CREATE_DATE_TIME = "create_date_time";
    public static final String UPDATE_DATE_TIME = "update_date_time";
    
    // Amex specific fields
    public static final String AMEX_SE_NUMBER = "amex_se_number";
    public static final String AMEX_MERCHANT_ID = "amex_merchant_id";
    public static final String AMEX_CARD_MEMBER_NUMBER = "amex_card_member_number";
    public static final String AMEX_TRANSACTION_CODE = "amex_transaction_code";
    public static final String AMEX_REFERENCE_NUMBER = "amex_reference_number";
    public static final String AMEX_APPROVAL_CODE = "amex_approval_code";
    public static final String AMEX_CHARGE_DATE = "amex_charge_date";
    public static final String AMEX_SUBMISSION_DATE = "amex_submission_date";
    
    // SMF specific fields
    public static final String SMF_RECORD_TYPE = "smf_record_type";
    public static final String SMF_SEQUENCE_NUMBER = "smf_sequence_number";
    public static final String SMF_FILE_DATE = "smf_file_date";
    public static final String SMF_MERCHANT_CATEGORY = "smf_merchant_category";
    public static final String SMF_PROCESSING_DATE = "smf_processing_date";
    public static final String SMF_SETTLEMENT_FLAG = "smf_settlement_flag";
    
    // Merchant information fields
    public static final String MERCHANT_CATEGORY_CODE = "merchant_category_code";
    public static final String MERCHANT_COUNTRY_CODE = "merchant_country_code";
    public static final String MERCHANT_CITY = "merchant_city";
    public static final String MERCHANT_STATE = "merchant_state";
    public static final String MERCHANT_ZIP_CODE = "merchant_zip_code";
    public static final String MERCHANT_PHONE = "merchant_phone";
    public static final String MERCHANT_DBA_NAME = "merchant_dba_name";
    
    // Status and flag constants
    public static final String ACTIVE_FLAG = "Y";
    public static final String INACTIVE_FLAG = "N";
    public static final String PROCESSED_FLAG = "P";
    public static final String ERROR_FLAG = "E";
    public static final String SUCCESS_STATUS = "SUCCESS";
    public static final String FAILED_STATUS = "FAILED";
    
    // Lookup values
    public static final String DMX_LOOKUP_FAILURE = "-2";
    public static final String DMX_LOOKUP_NULL_BLANK = "-1";
    public static final String DEFAULT_VALUE = "UNKNOWN";
    
    // Currency constants
    public static final String GBP_CURRENCY = "GBP";
    public static final String USD_CURRENCY = "USD";
    public static final String EUR_CURRENCY = "EUR";
    
    // Table names and queries
    public static final String AMEX_UK_TRANSACTION_TABLE = "trusted_layer.amex_uk_transactions";
    public static final String AMEX_UK_SMF_OUTPUT_TABLE = "transformed_layer.amex_uk_smf_output";
    
    // Query constants
    public static final String AMEX_MERCHANT_INFO_QUERY = 
        "SELECT merchant_number, merchant_name, merchant_category_code, " +
        "merchant_country_code, merchant_city, merchant_state, merchant_zip_code, " +
        "merchant_phone, merchant_dba_name, active_flag " +
        "FROM transformed_layer.dim_merchant_information " +
        "WHERE active_flag = 'Y' AND merchant_type = 'AMEX'";
    
    public static final String AMEX_CURRENCY_RATES_QUERY = 
        "SELECT from_currency_code, to_currency_code, exchange_rate, effective_date " +
        "FROM transformed_layer.dim_currency_rates " +
        "WHERE active_flag = 'Y' AND rate_type = 'SETTLEMENT'";
    
    // Record type constants for SMF format
    public static final String SMF_HEADER_RECORD = "01";
    public static final String SMF_TRANSACTION_RECORD = "02";
    public static final String SMF_SUMMARY_RECORD = "03";
    public static final String SMF_TRAILER_RECORD = "99";
    
    // File processing constants
    public static final int MAX_RECORDS_PER_FILE = 10000;
    public static final String FILE_EXTENSION_SMF = ".smf";
    public static final String FILE_PREFIX_AMEX_UK = "AMEX_UK_";
    
    // Validation constants
    public static final int MIN_MERCHANT_NUMBER_LENGTH = 10;
    public static final int MAX_MERCHANT_NUMBER_LENGTH = 15;
    public static final int TRANSACTION_ID_LENGTH = 20;
    public static final int AMEX_CARD_NUMBER_LENGTH = 15;
}