package com.globalpayments.batch.pipeline.util;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Query translator for Amex UK SMF File Generation Pipeline
 * Handles dynamic query generation with runtime parameters
 */
public class AmexQueryTranslator implements SerializableFunction<String, String>, AmexCommonUtil {
    
    private final String tableName;
    
    public AmexQueryTranslator(String tableName) {
        this.tableName = tableName;
    }
    
    @Override
    public String apply(String batchId) {
        switch(tableName) {
            case AMEX_UK_TRANSACTION_TABLE:
                return buildAmexTransactionQuery(batchId);
            case AMEX_UK_SMF_OUTPUT_TABLE:
                return buildSMFOutputTableName(batchId);
            default:
                throw new IllegalArgumentException("Unknown table name: " + tableName);
        }
    }
    
    /**
     * Build the main Amex UK transaction query
     */
    private String buildAmexTransactionQuery(String batchId) {
        return String.format(
            "SELECT " +
            "  transaction_id, " +
            "  merchant_number, " +
            "  transaction_date, " +
            "  transaction_time, " +
            "  transaction_amount, " +
            "  currency_code, " +
            "  settlement_amount, " +
            "  settlement_currency, " +
            "  card_number, " +
            "  card_type, " +
            "  authorization_code, " +
            "  batch_number, " +
            "  terminal_id, " +
            "  amex_se_number, " +
            "  amex_merchant_id, " +
            "  amex_card_member_number, " +
            "  amex_transaction_code, " +
            "  amex_reference_number, " +
            "  amex_approval_code, " +
            "  amex_charge_date, " +
            "  amex_submission_date, " +
            "  etl_batch_date, " +
            "  create_date_time, " +
            "  update_date_time " +
            "FROM `%s` " +
            "WHERE etl_batch_date = '%s' " +
            "  AND card_type = 'AMEX' " +
            "  AND transaction_amount > 0 " +
            "  AND merchant_number IS NOT NULL " +
            "  AND transaction_id IS NOT NULL " +
            "ORDER BY transaction_date, transaction_time, transaction_id",
            AMEX_UK_TRANSACTION_TABLE, 
            batchId
        );
    }
    
    /**
     * Build the SMF output table name with batch partitioning
     */
    private String buildSMFOutputTableName(String batchId) {
        return String.format("%s$%s", AMEX_UK_SMF_OUTPUT_TABLE, batchId.replace("-", ""));
    }
    
    /**
     * Static method to get merchant information query
     */
    public static String getMerchantInfoQuery() {
        return AMEX_MERCHANT_INFO_QUERY;
    }
    
    /**
     * Static method to get currency rates query
     */
    public static String getCurrencyRatesQuery() {
        return AMEX_CURRENCY_RATES_QUERY;
    }
    
    /**
     * Build query for merchant lookup with specific criteria
     */
    public static String buildMerchantLookupQuery(String merchantNumber) {
        return String.format(
            "SELECT * FROM transformed_layer.dim_merchant_information " +
            "WHERE merchant_number = '%s' AND active_flag = 'Y'",
            merchantNumber
        );
    }
    
    /**
     * Build query for currency conversion rates
     */
    public static String buildCurrencyConversionQuery(String fromCurrency, String toCurrency, String effectiveDate) {
        return String.format(
            "SELECT exchange_rate FROM transformed_layer.dim_currency_rates " +
            "WHERE from_currency_code = '%s' " +
            "  AND to_currency_code = '%s' " +
            "  AND effective_date <= '%s' " +
            "  AND active_flag = 'Y' " +
            "ORDER BY effective_date DESC " +
            "LIMIT 1",
            fromCurrency, toCurrency, effectiveDate
        );
    }
}