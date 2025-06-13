package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import com.google.api.services.bigquery.model.TableRow;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;

/**
 * Merchant enrichment transform
 * Enriches transaction data with merchant information from side input
 */
public class MerchantEnrichment extends DoFn<TableRow, TableRow> implements AmexCommonUtil {
    
    private final String inputKey;
    private final String lookupKey;
    private final PCollectionView<Map<String, Iterable<TableRow>>> merchantView;
    
    public MerchantEnrichment(String inputKey, String lookupKey, 
                            PCollectionView<Map<String, Iterable<TableRow>>> merchantView) {
        this.inputKey = inputKey;
        this.lookupKey = lookupKey;
        this.merchantView = merchantView;
    }
    
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow inputRow = c.element();
        Map<String, Iterable<TableRow>> merchantData = c.sideInput(merchantView);
        
        if (inputRow.containsKey(inputKey)) {
            String merchantNumber = inputRow.get(inputKey).toString();
            
            if (merchantData.containsKey(merchantNumber)) {
                // Get first match from the iterable
                TableRow merchantInfo = merchantData.get(merchantNumber).iterator().next();
                
                // Enrich with merchant information
                inputRow.set(MERCHANT_NAME, 
                    merchantInfo.get(MERCHANT_NAME) != null ? 
                    merchantInfo.get(MERCHANT_NAME).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_CATEGORY_CODE, 
                    merchantInfo.get(MERCHANT_CATEGORY_CODE) != null ? 
                    merchantInfo.get(MERCHANT_CATEGORY_CODE).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_COUNTRY_CODE, 
                    merchantInfo.get(MERCHANT_COUNTRY_CODE) != null ? 
                    merchantInfo.get(MERCHANT_COUNTRY_CODE).toString() : "GB");
                
                inputRow.set(MERCHANT_CITY, 
                    merchantInfo.get(MERCHANT_CITY) != null ? 
                    merchantInfo.get(MERCHANT_CITY).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_STATE, 
                    merchantInfo.get(MERCHANT_STATE) != null ? 
                    merchantInfo.get(MERCHANT_STATE).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_ZIP_CODE, 
                    merchantInfo.get(MERCHANT_ZIP_CODE) != null ? 
                    merchantInfo.get(MERCHANT_ZIP_CODE).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_PHONE, 
                    merchantInfo.get(MERCHANT_PHONE) != null ? 
                    merchantInfo.get(MERCHANT_PHONE).toString() : DEFAULT_VALUE);
                
                inputRow.set(MERCHANT_DBA_NAME, 
                    merchantInfo.get(MERCHANT_DBA_NAME) != null ? 
                    merchantInfo.get(MERCHANT_DBA_NAME).toString() : DEFAULT_VALUE);
                
                inputRow.set("merchant_enrichment_status", SUCCESS_STATUS);
            } else {
                // Set default values when merchant not found
                inputRow.set(MERCHANT_NAME, DEFAULT_VALUE);
                inputRow.set(MERCHANT_CATEGORY_CODE, DEFAULT_VALUE);
                inputRow.set(MERCHANT_COUNTRY_CODE, "GB");
                inputRow.set(MERCHANT_CITY, DEFAULT_VALUE);
                inputRow.set(MERCHANT_STATE, DEFAULT_VALUE);
                inputRow.set(MERCHANT_ZIP_CODE, DEFAULT_VALUE);
                inputRow.set(MERCHANT_PHONE, DEFAULT_VALUE);
                inputRow.set(MERCHANT_DBA_NAME, DEFAULT_VALUE);
                inputRow.set("merchant_enrichment_status", FAILED_STATUS);
            }
        }
        
        c.output(inputRow);
    }
}