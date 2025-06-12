package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;

/**
 * Data validation transform for Amex UK transactions
 * Validates required fields and data quality
 */
public class DataValidation extends PTransform<PCollection<TableRow>, PCollection<TableRow>> 
    implements AmexCommonUtil {
    
    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
        return input.apply("Validate Transaction Data", ParDo.of(new DoFn<TableRow, TableRow>() {
            
            @ProcessElement
            public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
                
                // Validate required fields
                if (isValidTransaction(row)) {
                    // Add validation timestamp
                    row.set("validation_timestamp", System.currentTimeMillis());
                    row.set("validation_status", SUCCESS_STATUS);
                    out.output(row);
                } else {
                    // Log validation failure but don't output
                    System.err.println("Validation failed for transaction: " + 
                        row.get(TRANSACTION_ID));
                }
            }
            
            private boolean isValidTransaction(TableRow row) {
                // Check required fields
                if (row.get(TRANSACTION_ID) == null || 
                    row.get(TRANSACTION_ID).toString().trim().isEmpty()) {
                    return false;
                }
                
                if (row.get(MERCHANT_NUMBER) == null || 
                    row.get(MERCHANT_NUMBER).toString().trim().isEmpty()) {
                    return false;
                }
                
                if (row.get(TRANSACTION_DATE) == null) {
                    return false;
                }
                
                if (row.get(TRANSACTION_AMOUNT) == null) {
                    return false;
                }
                
                // Validate transaction amount is positive
                try {
                    double amount = Double.parseDouble(row.get(TRANSACTION_AMOUNT).toString());
                    if (amount <= 0) {
                        return false;
                    }
                } catch (NumberFormatException e) {
                    return false;
                }
                
                // Validate merchant number length
                String merchantNumber = row.get(MERCHANT_NUMBER).toString();
                if (merchantNumber.length() < MIN_MERCHANT_NUMBER_LENGTH || 
                    merchantNumber.length() > MAX_MERCHANT_NUMBER_LENGTH) {
                    return false;
                }
                
                // Validate transaction ID length
                String transactionId = row.get(TRANSACTION_ID).toString();
                if (transactionId.length() != TRANSACTION_ID_LENGTH) {
                    return false;
                }
                
                return true;
            }
        }));
    }
}