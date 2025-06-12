package com.globalpayments.batch.pipeline.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Amex UK SMF File Generation Pipeline
 * Contains helper methods and field mappings
 */
public class AmexUtility implements AmexCommonUtil {
    
    /**
     * Get the required column list for merchant information lookup
     * @return List of column names needed for merchant enrichment
     */
    public static List<String> getMerchantInfoColumnList() {
        return Arrays.asList(
            MERCHANT_NUMBER,
            MERCHANT_NAME,
            MERCHANT_CATEGORY_CODE,
            MERCHANT_COUNTRY_CODE,
            MERCHANT_CITY,
            MERCHANT_STATE,
            MERCHANT_ZIP_CODE,
            MERCHANT_PHONE,
            MERCHANT_DBA_NAME,
            ACTIVE_FLAG
        );
    }
    
    /**
     * Get the schema mapping for Amex transaction data transformation
     * Maps source fields to target fields for the pipeline
     * @return Map of field mappings
     */
    public static Map<String, String> getAmexTransactionSchema() {
        Map<String, String> fieldMapping = new HashMap<>();
        
        // Basic transaction fields
        fieldMapping.put(TRANSACTION_ID, TRANSACTION_ID);
        fieldMapping.put(MERCHANT_NUMBER, MERCHANT_NUMBER);
        fieldMapping.put(TRANSACTION_DATE, TRANSACTION_DATE);
        fieldMapping.put(TRANSACTION_TIME, TRANSACTION_TIME);
        fieldMapping.put(TRANSACTION_AMOUNT, TRANSACTION_AMOUNT);
        fieldMapping.put(CURRENCY_CODE, CURRENCY_CODE);
        fieldMapping.put(SETTLEMENT_AMOUNT, SETTLEMENT_AMOUNT);
        fieldMapping.put(SETTLEMENT_CURRENCY, SETTLEMENT_CURRENCY);
        
        // Amex specific fields
        fieldMapping.put(AMEX_SE_NUMBER, AMEX_SE_NUMBER);
        fieldMapping.put(AMEX_MERCHANT_ID, AMEX_MERCHANT_ID);
        fieldMapping.put(AMEX_CARD_MEMBER_NUMBER, AMEX_CARD_MEMBER_NUMBER);
        fieldMapping.put(AMEX_TRANSACTION_CODE, AMEX_TRANSACTION_CODE);
        fieldMapping.put(AMEX_REFERENCE_NUMBER, AMEX_REFERENCE_NUMBER);
        fieldMapping.put(AMEX_APPROVAL_CODE, AMEX_APPROVAL_CODE);
        fieldMapping.put(AMEX_CHARGE_DATE, AMEX_CHARGE_DATE);
        fieldMapping.put(AMEX_SUBMISSION_DATE, AMEX_SUBMISSION_DATE);
        
        // Card information
        fieldMapping.put(CARD_NUMBER, CARD_NUMBER);
        fieldMapping.put(CARD_TYPE, CARD_TYPE);
        fieldMapping.put(AUTHORIZATION_CODE, AUTHORIZATION_CODE);
        
        // Processing fields
        fieldMapping.put(BATCH_NUMBER, BATCH_NUMBER);
        fieldMapping.put(TERMINAL_ID, TERMINAL_ID);
        fieldMapping.put(ETL_BATCH_DATE, ETL_BATCH_DATE);
        fieldMapping.put(CREATE_DATE_TIME, CREATE_DATE_TIME);
        fieldMapping.put(UPDATE_DATE_TIME, UPDATE_DATE_TIME);
        
        return fieldMapping;
    }
    
    /**
     * Get the SMF output schema for the final file format
     * @return Map of SMF field mappings
     */
    public static Map<String, String> getSMFOutputSchema() {
        Map<String, String> smfMapping = new HashMap<>();
        
        // SMF specific fields
        smfMapping.put(SMF_RECORD_TYPE, SMF_RECORD_TYPE);
        smfMapping.put(SMF_SEQUENCE_NUMBER, SMF_SEQUENCE_NUMBER);
        smfMapping.put(SMF_FILE_DATE, SMF_FILE_DATE);
        smfMapping.put(SMF_MERCHANT_CATEGORY, SMF_MERCHANT_CATEGORY);
        smfMapping.put(SMF_PROCESSING_DATE, SMF_PROCESSING_DATE);
        smfMapping.put(SMF_SETTLEMENT_FLAG, SMF_SETTLEMENT_FLAG);
        
        // Transaction data in SMF format
        smfMapping.put(TRANSACTION_ID, TRANSACTION_ID);
        smfMapping.put(MERCHANT_NUMBER, MERCHANT_NUMBER);
        smfMapping.put(TRANSACTION_AMOUNT, TRANSACTION_AMOUNT);
        smfMapping.put(SETTLEMENT_AMOUNT, SETTLEMENT_AMOUNT);
        smfMapping.put(CURRENCY_CODE, CURRENCY_CODE);
        smfMapping.put(SETTLEMENT_CURRENCY, SETTLEMENT_CURRENCY);
        smfMapping.put(TRANSACTION_DATE, TRANSACTION_DATE);
        
        return smfMapping;
    }
    
    /**
     * Get the required column list for currency conversion lookup
     * @return List of column names needed for currency conversion
     */
    public static List<String> getCurrencyRatesColumnList() {
        return Arrays.asList(
            "from_currency_code",
            "to_currency_code", 
            "exchange_rate",
            "effective_date",
            ACTIVE_FLAG
        );
    }
    
    /**
     * Get validation rules for Amex transaction data
     * @return Map of field validation rules
     */
    public static Map<String, Object> getValidationRules() {
        Map<String, Object> rules = new HashMap<>();
        
        rules.put("min_merchant_number_length", MIN_MERCHANT_NUMBER_LENGTH);
        rules.put("max_merchant_number_length", MAX_MERCHANT_NUMBER_LENGTH);
        rules.put("transaction_id_length", TRANSACTION_ID_LENGTH);
        rules.put("amex_card_number_length", AMEX_CARD_NUMBER_LENGTH);
        rules.put("required_fields", Arrays.asList(
            TRANSACTION_ID,
            MERCHANT_NUMBER,
            TRANSACTION_DATE,
            TRANSACTION_AMOUNT,
            CURRENCY_CODE
        ));
        
        return rules;
    }
    
    /**
     * Get the list of supported currencies for Amex UK processing
     * @return List of currency codes
     */
    public static List<String> getSupportedCurrencies() {
        return Arrays.asList(
            GBP_CURRENCY,
            USD_CURRENCY,
            EUR_CURRENCY,
            "CAD", // Canadian Dollar
            "AUD", // Australian Dollar
            "JPY", // Japanese Yen
            "CHF", // Swiss Franc
            "SEK", // Swedish Krona
            "NOK", // Norwegian Krone
            "DKK"  // Danish Krone
        );
    }
    
    /**
     * Get default values for missing or null fields
     * @return Map of default values
     */
    public static Map<String, String> getDefaultValues() {
        Map<String, String> defaults = new HashMap<>();
        
        defaults.put(MERCHANT_NAME, DEFAULT_VALUE);
        defaults.put(MERCHANT_CITY, DEFAULT_VALUE);
        defaults.put(MERCHANT_STATE, DEFAULT_VALUE);
        defaults.put(MERCHANT_COUNTRY_CODE, "GB"); // Default to UK
        defaults.put(CURRENCY_CODE, GBP_CURRENCY);
        defaults.put(SETTLEMENT_CURRENCY, GBP_CURRENCY);
        defaults.put(CARD_TYPE, "AMEX");
        defaults.put(SMF_SETTLEMENT_FLAG, ACTIVE_FLAG);
        
        return defaults;
    }
}