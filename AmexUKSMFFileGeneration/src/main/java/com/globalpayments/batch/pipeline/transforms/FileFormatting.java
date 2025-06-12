package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * File formatting transform for SMF output
 * Formats the processed data according to SMF (Settlement Management File) specifications
 */
public class FileFormatting extends DoFn<TableRow, TableRow> implements AmexCommonUtil {
    
    private static final DecimalFormat AMOUNT_FORMAT = new DecimalFormat("#0.00");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
        
        TableRow smfRow = new TableRow();
        
        // SMF Header information
        smfRow.set(SMF_RECORD_TYPE, SMF_TRANSACTION_RECORD);
        smfRow.set(SMF_SEQUENCE_NUMBER, generateSequenceNumber());
        smfRow.set(SMF_FILE_DATE, getCurrentDate());
        smfRow.set(SMF_PROCESSING_DATE, getCurrentDate());
        smfRow.set(SMF_SETTLEMENT_FLAG, ACTIVE_FLAG);
        
        // Transaction identification
        smfRow.set(TRANSACTION_ID, formatString(row.get(TRANSACTION_ID), TRANSACTION_ID_LENGTH));
        smfRow.set(MERCHANT_NUMBER, formatString(row.get(MERCHANT_NUMBER), MAX_MERCHANT_NUMBER_LENGTH));
        smfRow.set(AMEX_SE_NUMBER, formatString(row.get(AMEX_SE_NUMBER), 10));
        smfRow.set(AMEX_MERCHANT_ID, formatString(row.get(AMEX_MERCHANT_ID), 15));
        smfRow.set(AMEX_REFERENCE_NUMBER, formatString(row.get(AMEX_REFERENCE_NUMBER), 20));
        
        // Transaction details
        smfRow.set(TRANSACTION_DATE, formatDate(row.get(TRANSACTION_DATE)));
        smfRow.set(TRANSACTION_TIME, formatTime(row.get(TRANSACTION_TIME)));
        smfRow.set(TRANSACTION_AMOUNT, formatAmount(row.get(TRANSACTION_AMOUNT)));
        smfRow.set(SETTLEMENT_AMOUNT, formatAmount(row.get(SETTLEMENT_AMOUNT)));
        smfRow.set(CURRENCY_CODE, formatString(row.get(CURRENCY_CODE), 3));
        smfRow.set(SETTLEMENT_CURRENCY, formatString(row.get(SETTLEMENT_CURRENCY), 3));
        
        // Card information
        smfRow.set(CARD_NUMBER, maskCardNumber(row.get(CARD_NUMBER)));
        smfRow.set(CARD_TYPE, formatString(row.get(CARD_TYPE), 10));
        smfRow.set(AUTHORIZATION_CODE, formatString(row.get(AUTHORIZATION_CODE), 10));
        
        // Merchant information
        smfRow.set(MERCHANT_NAME, formatString(row.get(MERCHANT_NAME), 40));
        smfRow.set(MERCHANT_CATEGORY_CODE, formatString(row.get(MERCHANT_CATEGORY_CODE), 4));
        smfRow.set(MERCHANT_COUNTRY_CODE, formatString(row.get(MERCHANT_COUNTRY_CODE), 3));
        smfRow.set(MERCHANT_CITY, formatString(row.get(MERCHANT_CITY), 25));
        smfRow.set(MERCHANT_STATE, formatString(row.get(MERCHANT_STATE), 10));
        
        // Amex specific fields
        smfRow.set(AMEX_TRANSACTION_CODE, formatString(row.get(AMEX_TRANSACTION_CODE), 10));
        smfRow.set(AMEX_APPROVAL_CODE, formatString(row.get(AMEX_APPROVAL_CODE), 10));
        smfRow.set(AMEX_CHARGE_DATE, formatDate(row.get(AMEX_CHARGE_DATE)));
        smfRow.set(AMEX_SUBMISSION_DATE, formatDate(row.get(AMEX_SUBMISSION_DATE)));
        
        // Processing metadata
        smfRow.set(ETL_BATCH_DATE, formatDate(row.get(ETL_BATCH_DATE)));
        smfRow.set(CREATE_DATE_TIME, getCurrentDateTime());
        smfRow.set("smf_format_version", "1.0");
        smfRow.set("processing_status", SUCCESS_STATUS);
        
        out.output(smfRow);
    }
    
    private String formatString(Object value, int maxLength) {
        if (value == null) {
            return "";
        }
        String str = value.toString().trim();
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }
    
    private String formatAmount(Object value) {
        if (value == null) {
            return "0.00";
        }
        try {
            double amount = Double.parseDouble(value.toString());
            return AMOUNT_FORMAT.format(amount);
        } catch (NumberFormatException e) {
            return "0.00";
        }
    }
    
    private String formatDate(Object value) {
        if (value == null) {
            return getCurrentDate();
        }
        String dateStr = value.toString();
        // Assume input is in yyyy-MM-dd format, return as is
        if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return dateStr;
        }
        return getCurrentDate();
    }
    
    private String formatTime(Object value) {
        if (value == null) {
            return "00:00:00";
        }
        String timeStr = value.toString();
        // Assume input is in HH:mm:ss format, return as is
        if (timeStr.matches("\\d{2}:\\d{2}:\\d{2}")) {
            return timeStr;
        }
        return "00:00:00";
    }
    
    private String maskCardNumber(Object value) {
        if (value == null) {
            return "***************";
        }
        String cardNumber = value.toString();
        if (cardNumber.length() >= 4) {
            return "***********" + cardNumber.substring(cardNumber.length() - 4);
        }
        return "***************";
    }
    
    private String getCurrentDate() {
        return LocalDateTime.now().format(DATE_FORMATTER);
    }
    
    private String getCurrentDateTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    
    private String generateSequenceNumber() {
        // In production, this would be a proper sequence generator
        return String.valueOf(System.currentTimeMillis() % 1000000);
    }
}