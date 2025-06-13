package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;

/**
 * SMF Data Processing transform
 * Splits data based on processing requirements (currency conversion needed vs direct processing)
 */
public class SMFDataProcessing extends DoFn<TableRow, TableRow> implements AmexCommonUtil {
    
    private final TupleTag<TableRow> directProcessingTag;
    private final TupleTag<TableRow> currencyConversionTag;
    
    public SMFDataProcessing(TupleTag<TableRow> directProcessingTag, 
                           TupleTag<TableRow> currencyConversionTag) {
        this.directProcessingTag = directProcessingTag;
        this.currencyConversionTag = currencyConversionTag;
    }
    
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        
        // Check if currency conversion is needed
        if (needsCurrencyConversion(row)) {
            c.output(currencyConversionTag, row);
        } else {
            c.output(directProcessingTag, row);
        }
    }
    
    private boolean needsCurrencyConversion(TableRow row) {
        String transactionCurrency = row.get(CURRENCY_CODE) != null ? 
            row.get(CURRENCY_CODE).toString() : "";
        String settlementCurrency = row.get(SETTLEMENT_CURRENCY) != null ? 
            row.get(SETTLEMENT_CURRENCY).toString() : "";
        
        // If currencies are different, conversion is needed
        if (!transactionCurrency.equals(settlementCurrency)) {
            return true;
        }
        
        // If settlement currency is not GBP (UK default), conversion might be needed
        if (!GBP_CURRENCY.equals(settlementCurrency)) {
            return true;
        }
        
        // Check if settlement amount is missing or zero (needs calculation)
        if (row.get(SETTLEMENT_AMOUNT) == null) {
            return true;
        }
        
        try {
            double settlementAmount = Double.parseDouble(row.get(SETTLEMENT_AMOUNT).toString());
            if (settlementAmount == 0.0) {
                return true;
            }
        } catch (NumberFormatException e) {
            return true;
        }
        
        return false;
    }
}