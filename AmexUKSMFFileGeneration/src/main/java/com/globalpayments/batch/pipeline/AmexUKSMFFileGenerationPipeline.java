package com.globalpayments.batch.pipeline;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.globalpayments.airflow.AirflowOptions;
import com.globalpayments.batch.pipeline.transforms.AmexTransactionFilter;
import com.globalpayments.batch.pipeline.transforms.CurrencyConversion;
import com.globalpayments.batch.pipeline.transforms.DataValidation;
import com.globalpayments.batch.pipeline.transforms.FileFormatting;
import com.globalpayments.batch.pipeline.transforms.MerchantEnrichment;
import com.globalpayments.batch.pipeline.transforms.SMFDataProcessing;
import com.globalpayments.batch.pipeline.util.AmexCommonUtil;
import com.globalpayments.batch.pipeline.util.AmexQueryTranslator;
import com.globalpayments.batch.pipeline.util.AmexUtility;
import com.google.api.services.bigquery.model.TableRow;

/**
 * AmexUKSMFFileGeneration Pipeline
 * 
 * This pipeline processes Amex UK transaction data and generates SMF (Settlement Management File) 
 * format files for settlement processing. The pipeline performs the following operations:
 * 1. Reads Amex UK transaction data from BigQuery
 * 2. Filters and validates transaction records
 * 3. Enriches data with merchant information
 * 4. Performs currency conversion where needed
 * 5. Formats data according to SMF specifications
 * 6. Outputs processed files for settlement
 */
public class AmexUKSMFFileGenerationPipeline implements AmexCommonUtil {

    public static void main(String[] args) {
        
        AirflowOptions airflowOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(AirflowOptions.class);
        Pipeline pipeline = Pipeline.create(airflowOptions);
        
        // Read Amex UK transaction data from BigQuery
        PCollection<TableRow> amexTransactionData = pipeline
                .apply("Read Amex UK Transaction Data",
                    BigQueryIO.readTableRows().withoutValidation().withTemplateCompatibility()
                    .fromQuery(NestedValueProvider.of(airflowOptions.getJobBatchId(), 
                        new AmexQueryTranslator(AMEX_UK_TRANSACTION_TABLE)))
                    .usingStandardSql())
                .apply("Apply Initial Data Validation", ParDo.of(new DataValidation()));
        
        // Filter for Amex-specific transactions
        PCollection<TableRow> filteredAmexData = amexTransactionData
                .apply("Filter Amex Transactions", ParDo.of(new AmexTransactionFilter()));
        
        // Create side input for merchant information lookup
        PCollectionView<Map<String, Iterable<TableRow>>> merchantInfoView = pipeline
                .apply("Read Merchant Information", BigQueryIO.readTableRows()
                        .fromQuery(AMEX_MERCHANT_INFO_QUERY)
                        .usingStandardSql().withoutResultFlattening().withoutValidation()
                        .withTemplateCompatibility())
                .apply("Convert Merchant Info to View", 
                    new com.globalpayments.batch.pipeline.transforms.FetchRequiredDataAndConvertToView(
                        "merchant_number", AmexUtility.getMerchantInfoColumnList()));
        
        // Enrich data with merchant information
        PCollection<TableRow> enrichedData = filteredAmexData
                .apply("Enrich with Merchant Data", 
                    ParDo.of(new MerchantEnrichment("merchant_number", "merchant_number", merchantInfoView))
                    .withSideInputs(merchantInfoView));
        
        // Split data into different processing paths
        final TupleTag<TableRow> currencyConversionTag = new TupleTag<TableRow>() {
            private static final long serialVersionUID = 1L;
        };
        final TupleTag<TableRow> directProcessingTag = new TupleTag<TableRow>() {
            private static final long serialVersionUID = 1L;
        };
        
        PCollectionTuple splitData = enrichedData
                .apply("Split Data by Processing Requirements", 
                    ParDo.of(new SMFDataProcessing(directProcessingTag, currencyConversionTag))
                    .withOutputTags(directProcessingTag, TupleTagList.of(currencyConversionTag)));
        
        // Process currency conversion path
        PCollection<TableRow> convertedCurrencyData = splitData.get(currencyConversionTag)
                .apply("Perform Currency Conversion", ParDo.of(new CurrencyConversion()));
        
        // Merge both processing paths
        PCollectionList<TableRow> allProcessedData = PCollectionList.of(splitData.get(directProcessingTag))
                .and(convertedCurrencyData);
        
        PCollection<TableRow> finalProcessedData = allProcessedData
                .apply("Merge Processed Data", Flatten.pCollections());
        
        // Format data for SMF output
        PCollection<TableRow> smfFormattedData = finalProcessedData
                .apply("Format for SMF Output", ParDo.of(new FileFormatting()));
        
        // Write to BigQuery output table
        smfFormattedData.apply("Write SMF Data to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(NestedValueProvider.of(airflowOptions.getJobBatchId(), 
                            new AmexQueryTranslator(AMEX_UK_SMF_OUTPUT_TABLE)))
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        
        pipeline.run();
    }
}