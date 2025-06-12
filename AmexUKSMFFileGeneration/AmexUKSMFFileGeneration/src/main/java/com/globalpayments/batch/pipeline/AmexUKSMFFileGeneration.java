package com.globalpayments.batch.pipeline;

import com.globalpayments.airflow.AirflowOptions;
import com.globalpayments.batch.pipeline.transforms.*;
import com.globalpayments.batch.pipeline.utils.Constants;
import com.globalpayments.batch.pipeline.utils.QueryTranslator;
import com.globalpayments.batch.pipeline.utils.StringAsciiCoder;
import com.globalpayments.batch.pipeline.utils.Utility;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class AmexUKSMFFileGeneration {
    public static final Logger log= LoggerFactory.getLogger(AmexUKSMFFileGeneration.class);
    public static void main(String[] args) {

        //tupletag for today's data
        TupleTag<String> todaysDataTuple = new TupleTag<String>("todaysDataTuple") {
            private static final long serialVersionUID = 3919729947021160434L;
        };

        //tupletag for pending data
        TupleTag<String> pendingDataTuple = new TupleTag<String>("pendingDataTuple") {
            private static final long serialVersionUID = 6212729898624608077L;
        };

        //tupletag for next day's pending data
        TupleTag<String> pendingForNextDayTuple = new TupleTag<String>("pendingForNextDayTuple") {
            private static final long serialVersionUID = -3769412224976703896L;
        };

        AirflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AirflowOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<TableRow> amexUKSmfData = p.apply("Read AMEX UK Data",
                BigQueryIO.readTableRows().withoutValidation().withTemplateCompatibility()
                        .fromQuery(ValueProvider.NestedValueProvider.of(options.getEtlbatchid(),
                                new QueryTranslator(Constants.AMEX_UK_BASE_QUERY)))
                        .usingStandardSql());


        PCollectionView<Long> recordCountView = amexUKSmfData.apply("Finding record count", Count.globally())
                .apply(View.asSingleton());

        PCollection<String> dtlRecord = amexUKSmfData.apply("converting DTL records into generic tableRow",
                ParDo.of(new ConvertTableRowToString(Utility.getDTLColumnList())));

        PCollection<String> dt2Record = amexUKSmfData.apply("converting DT2 records into generic tableRow",
                ParDo.of(new ConvertTableRowToString(Utility.getDT2ColumnList())));


        PCollectionList<String> amexUkSmfList = PCollectionList.of(dtlRecord).and(dt2Record);

        PCollectionView<Map<String, Iterable<String>>> amexUkSmfRecord = amexUkSmfList
                .apply("merging merchant_no null record", Flatten.pCollections())
                .apply("KV for sorting", ParDo.of(new KVForSorting())).apply("GBK operation", GroupByKey.create())
                .apply(View.asMap());

        // Read pending file
        PCollectionView<Map<String, Iterable<String>>> pendingDataMap = p
                .apply("Reading pending data's file", TextIO.read().from(options.getPendingDataFile()))
                .apply("KV for sorting", ParDo.of(new ConvertToKV())).apply("GBK operation", GroupByKey.create())
                .apply(View.asMap());


        //Removing duplicate merchant record from pending and today's data
        PCollectionTuple finalData = p.apply("Control Thread", Create.of("Control Thread")).apply("Sorting Records",
                ParDo.of(new CombineDataFn(amexUkSmfRecord, recordCountView, pendingDataMap,
                                options.getPendingRecordCount(), todaysDataTuple, pendingDataTuple,pendingForNextDayTuple))
                        .withSideInputs(amexUkSmfRecord, recordCountView, pendingDataMap)
                        .withOutputTags(todaysDataTuple, TupleTagList.of(pendingDataTuple).and(pendingForNextDayTuple)));

        PCollectionView<Map<String, Iterable<String>>> pendingDataMapView = finalData.get(pendingDataTuple)
                .apply("KV for sorting", ParDo.of(new SplitAndConvertToKV())).apply("GBK operation", GroupByKey.create())
                .apply(View.asMap());

        PCollectionView<Map<String, Iterable<String>>> todaysDataMapView = finalData.get(todaysDataTuple)
                .apply("KV for sorting", ParDo.of(new SplitAndConvertToKV())).apply("GBK operation", GroupByKey.create())
                .apply(View.asMap());

        PCollectionView<List<String>> pendingForNextDayView = finalData.get(pendingForNextDayTuple)
                .apply(View.asList());

        //tuple tag for today's final data
        TupleTag<String> todaysFinalDataTuple = new TupleTag<String>("todaysFinalDataTuple") {
            private static final long serialVersionUID = 3233608754765139031L;
        };

        //tuple tag for pending final data
        TupleTag<String> pendingFinalDataTuple = new TupleTag<String>("pendingFinalDataTuple") {
            private static final long serialVersionUID = -2616316272238890035L;
        };



        // File Generation with pending and today's data
        PCollectionTuple outData = p.apply(Create.of("TEMP")).apply("File Generation",
                ParDo.of(new FileGeneration(todaysDataMapView, recordCountView, pendingDataMapView,
                                options.getPendingRecordCount(), todaysFinalDataTuple, pendingFinalDataTuple,pendingForNextDayView))
                        .withSideInputs(todaysDataMapView, recordCountView, pendingDataMapView, pendingForNextDayView)
                        .withOutputTags(todaysFinalDataTuple, TupleTagList.of(pendingFinalDataTuple)));



        // Write today's data into file
        outData.get(todaysFinalDataTuple).setCoder(StringAsciiCoder.of()).apply("WRITE DATA TO FILE",
                TextIO.write().to(options.getOutputFilePath()).withoutSharding());

        // Write pending data into file
        outData.get(pendingFinalDataTuple).setCoder(StringAsciiCoder.of()).apply("WRITE DATA TO PENDING DATA FILE",
                TextIO.write().to(options.getTargetPendingDataFile()).withoutSharding());


        // Pending Data Count View
        PCollectionView<Long> pendingRecordCounterView = outData.get(pendingFinalDataTuple)
                .apply("Find the count of Pending records",ParDo.of(new SplitElements()))
                .apply(View.asSingleton());


        PCollectionView<List<String>> appConfigView = p
                .apply("Read AppConfig from GCS", TextIO.read().from(options.getConfigPath()))
                .apply("AppConfig View", View.asList());

        PCollectionView<TableRow> configuration = p.apply("Fetching Configuration ", Create.of("TempOne"))
                .apply("Get Configuration", ParDo.of(new GetConfig(appConfigView)).withSideInputs(appConfigView))
                .apply("Configuration View", View.asSingleton());

        // Update record count for target pending data file
        p.apply("Audit Record count ",
                Create.of("TempTwo")).apply(
                "Update Audit in Cloud SQL", ParDo
                        .of(new UpdateAmexUKPendingConfig(configuration, options.getTargetPendingDataFile(),
                                pendingRecordCounterView))
                        .withSideInputs(configuration, pendingRecordCounterView));


        p.run();

        log.info("Pipeline completed successfully.");

    }



}
