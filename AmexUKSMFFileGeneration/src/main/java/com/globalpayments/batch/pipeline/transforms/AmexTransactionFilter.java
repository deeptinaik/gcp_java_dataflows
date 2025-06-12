package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;

/**
 * Filter transform for Amex-specific transactions
 * Filters and processes only Amex card transactions
 */
public class AmexTransactionFilter extends PTransform<PCollection<TableRow>, PCollection<TableRow>> 
    implements AmexCommonUtil {
    
    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
        return input.apply("Filter Amex Transactions", ParDo.of(new DoFn<TableRow, TableRow>() {
            
            @ProcessElement
            public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
                
                if (isAmexTransaction(row)) {
                    // Enrich with Amex-specific processing flags
                    row.set("amex_processed_flag", ACTIVE_FLAG);
                    row.set("processing_timestamp", System.currentTimeMillis());
                    
                    // Set default values for missing Amex fields
                    setDefaultAmexValues(row);
                    
                    out.output(row);
                }
            }
            
            private boolean isAmexTransaction(TableRow row) {
                // Check if card type is Amex
                String cardType = row.get(CARD_TYPE) != null ? 
                    row.get(CARD_TYPE).toString().toUpperCase() : "";
                
                if (!"AMEX".equals(cardType) && !"AMERICAN EXPRESS".equals(cardType)) {
                    return false;
                }
                
                // Check if card number follows Amex pattern (starts with 3)
                String cardNumber = row.get(CARD_NUMBER) != null ? 
                    row.get(CARD_NUMBER).toString() : "";
                
                if (!cardNumber.startsWith("3") || cardNumber.length() != AMEX_CARD_NUMBER_LENGTH) {
                    return false;
                }
                
                // Check if merchant accepts Amex
                String merchantNumber = row.get(MERCHANT_NUMBER) != null ? 
                    row.get(MERCHANT_NUMBER).toString() : "";
                
                // Amex merchant numbers typically have specific patterns
                if (merchantNumber.isEmpty()) {
                    return false;
                }
                
                return true;
            }
            
            private void setDefaultAmexValues(TableRow row) {
                // Set default Amex SE number if missing
                if (row.get(AMEX_SE_NUMBER) == null) {
                    row.set(AMEX_SE_NUMBER, "0000000");
                }
                
                // Set default Amex merchant ID if missing
                if (row.get(AMEX_MERCHANT_ID) == null) {
                    row.set(AMEX_MERCHANT_ID, row.get(MERCHANT_NUMBER));
                }
                
                // Set default transaction code if missing
                if (row.get(AMEX_TRANSACTION_CODE) == null) {
                    row.set(AMEX_TRANSACTION_CODE, "SALE");
                }
                
                // Set default reference number if missing
                if (row.get(AMEX_REFERENCE_NUMBER) == null) {
                    row.set(AMEX_REFERENCE_NUMBER, row.get(TRANSACTION_ID));
                }
                
                // Set default approval code if missing
                if (row.get(AMEX_APPROVAL_CODE) == null) {
                    row.set(AMEX_APPROVAL_CODE, row.get(AUTHORIZATION_CODE));
                }
                
                // Set charge date to transaction date if missing
                if (row.get(AMEX_CHARGE_DATE) == null) {
                    row.set(AMEX_CHARGE_DATE, row.get(TRANSACTION_DATE));
                }
                
                // Set submission date to current date if missing
                if (row.get(AMEX_SUBMISSION_DATE) == null) {
                    row.set(AMEX_SUBMISSION_DATE, 
                        java.time.LocalDate.now().format(
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                }
            }
        }));
    }
}