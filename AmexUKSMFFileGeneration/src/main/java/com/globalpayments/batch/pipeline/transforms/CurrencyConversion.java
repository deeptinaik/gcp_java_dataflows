package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;

/**
 * Currency conversion transform
 * Performs currency conversion for transactions that need it
 */
public class CurrencyConversion extends DoFn<TableRow, TableRow> implements AmexCommonUtil {
    
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
        
        // Get currency information
        String transactionCurrency = row.get(CURRENCY_CODE) != null ? 
            row.get(CURRENCY_CODE).toString() : GBP_CURRENCY;
        String settlementCurrency = row.get(SETTLEMENT_CURRENCY) != null ? 
            row.get(SETTLEMENT_CURRENCY).toString() : GBP_CURRENCY;
        
        // Get transaction amount
        double transactionAmount = 0.0;
        try {
            transactionAmount = Double.parseDouble(row.get(TRANSACTION_AMOUNT).toString());
        } catch (NumberFormatException e) {
            // Log error and use 0
            System.err.println("Invalid transaction amount: " + row.get(TRANSACTION_AMOUNT));
        }
        
        // Calculate settlement amount
        double settlementAmount = calculateSettlementAmount(
            transactionAmount, transactionCurrency, settlementCurrency);
        
        // Update the row with converted amounts
        row.set(SETTLEMENT_AMOUNT, settlementAmount);
        row.set(SETTLEMENT_CURRENCY, settlementCurrency);
        
        // Add conversion metadata
        row.set("conversion_rate", getExchangeRate(transactionCurrency, settlementCurrency));
        row.set("conversion_timestamp", System.currentTimeMillis());
        row.set("conversion_status", SUCCESS_STATUS);
        
        out.output(row);
    }
    
    private double calculateSettlementAmount(double transactionAmount, 
                                           String fromCurrency, String toCurrency) {
        
        // If same currency, no conversion needed
        if (fromCurrency.equals(toCurrency)) {
            return transactionAmount;
        }
        
        // Get exchange rate
        double exchangeRate = getExchangeRate(fromCurrency, toCurrency);
        
        // Calculate converted amount
        return transactionAmount * exchangeRate;
    }
    
    private double getExchangeRate(String fromCurrency, String toCurrency) {
        // In a real implementation, this would lookup rates from a side input or external service
        // For demo purposes, using fixed rates
        
        if (fromCurrency.equals(toCurrency)) {
            return 1.0;
        }
        
        // Mock exchange rates (in real implementation, get from dim_currency_rates table)
        if (USD_CURRENCY.equals(fromCurrency) && GBP_CURRENCY.equals(toCurrency)) {
            return 0.79; // USD to GBP
        } else if (EUR_CURRENCY.equals(fromCurrency) && GBP_CURRENCY.equals(toCurrency)) {
            return 0.86; // EUR to GBP
        } else if (GBP_CURRENCY.equals(fromCurrency) && USD_CURRENCY.equals(toCurrency)) {
            return 1.27; // GBP to USD
        } else if (GBP_CURRENCY.equals(fromCurrency) && EUR_CURRENCY.equals(toCurrency)) {
            return 1.16; // GBP to EUR
        } else {
            // Default rate for unsupported conversions
            return 1.0;
        }
    }
}