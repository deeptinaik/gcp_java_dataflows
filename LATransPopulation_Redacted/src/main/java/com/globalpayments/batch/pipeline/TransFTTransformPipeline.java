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
import com.globalpayments.batch.pipeline.pmanager.TransFTProcessing;

import com.globalpayments.batch.pipeline.transforms.CleaningData;
import com.globalpayments.batch.pipeline.transforms.CleaningSettlementData;
import com.globalpayments.batch.pipeline.transforms.FetchRequiredDataAndConvertToView;
import com.globalpayments.batch.pipeline.transforms.FieldTransformation;
import com.globalpayments.batch.pipeline.transforms.FilterCurrencyCodeByAlphaAndNumeric;
import com.globalpayments.batch.pipeline.transforms.FilterSettlementCurrencyCodeByAlphaAndNumeric;
import com.globalpayments.batch.pipeline.transforms.FinalLeftOuterJoinSideInputCurrencyCode;
import com.globalpayments.batch.pipeline.transforms.FinalSettleLeftOuterJoinSideInputCurrencyCode;
import com.globalpayments.batch.pipeline.transforms.LeftOuterJoinSideInput;
import com.globalpayments.batch.pipeline.transforms.LeftOuterJoinSideInputCurrencyCode;
import com.globalpayments.batch.pipeline.transforms.SplittingCurrenCodeInvalidValid;
import com.globalpayments.batch.pipeline.transforms.SplittingSettlementCurrencyCodeInvalidValid;
import com.globalpayments.batch.pipeline.transforms.TransFTFinalFilter;
import com.globalpayments.batch.pipeline.util.CommonUtil;

import com.globalpayments.batch.pipeline.util.QueryTranslator;
import com.globalpayments.batch.pipeline.util.QueryTranslatorDimMerchantInformation;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TransFTTransformPipeline  implements CommonUtil {

		public static void main(String[] args) {

			AirflowOptions airflowOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(AirflowOptions.class);
			Pipeline pipeline = Pipeline.create(airflowOptions);
			//airflowOptions.setWorkerMachineType("n1-standard-4");
			
			
			// Fetching transft trusted layer data from BigQuery
			PCollection<TableRow> transFTInputData = pipeline
					.apply("Transft_intake_table Table",
					BigQueryIO.readTableRows().withoutValidation().withTemplateCompatibility()
					.fromQuery(NestedValueProvider.of(airflowOptions.getJobBatchId(),new QueryTranslator(TRANSFT_INTAKE_TABLE)))
					.usingStandardSql()).apply("Applying transformation", ParDo.of(new	FieldTransformation()));
			
			
			// TransFT transform processing
			PCollection<TableRow> transFTRecords = pipeline.apply("TransFT table data",new TransFTProcessing(transFTInputData));
			
			
			PCollectionView<Map<String, Iterable<TableRow>>> dimMerchantInfoView = pipeline
					.apply("Reading Data From dim_merchant_information", BigQueryIO.readTableRows()
							.fromQuery(NestedValueProvider.of(airflowOptions.getJobBatchId(),
									new QueryTranslatorDimMerchantInformation()))
							.usingStandardSql().withoutResultFlattening().withoutValidation().withTemplateCompatibility())
					.apply("Filtering And Converting To View", new FetchRequiredDataAndConvertToView("dmi_merchant_number",
						    Utility.getDimMerchantInfoRequiredColumnList()));

			
			
			TupleTag<TableRow> invalidCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			TupleTag<TableRow> validCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			
			

			PCollectionTuple currencyCodeInvalidValidtuple = transFTRecords.apply("Splitting Currency Code By Invalid Valid", ParDo
							.of(new SplittingCurrenCodeInvalidValid(validCurrencyCodeTupleTag, invalidCurrencyCodeTupleTag))
							.withOutputTags(validCurrencyCodeTupleTag, TupleTagList.of(invalidCurrencyCodeTupleTag)));
			
			
			PCollection<TableRow> merchantInfoSkJoinResult = currencyCodeInvalidValidtuple.get(invalidCurrencyCodeTupleTag)
					.apply("Performing Left Outer Join With dim_merchant_information", ParDo
							.of(new LeftOuterJoinSideInput("merchant_number_int", "dmi_merchant_number", dimMerchantInfoView))
							.withSideInputs(dimMerchantInfoView));
			
			
			PCollectionView<Map<String, Iterable<TableRow>>> dimDefaultCurrencyView = pipeline
					.apply("reading dim_default_currency",
							BigQueryIO.readTableRows().fromQuery(CommonUtil.DIM_DEFAULT_CURRENCY_QUERY)
									.usingStandardSql().withoutResultFlattening().withoutValidation()
									.withTemplateCompatibility())
					.apply("Filtering And Converting To View", new FetchRequiredDataAndConvertToView("ddc_corporate_region",
							Utility.getDefaultCurrenciesLkpColumnList()));
			
			
			PCollection<TableRow> dimDefaultCurrencyLookupJoinResult = merchantInfoSkJoinResult.apply(
					"Performing Join with DIM_DEFAULT_CURRENCY",
					ParDo.of(new LeftOuterJoinSideInputCurrencyCode("dmi_corporate_region", "ddc_corporate_region",
							dimDefaultCurrencyView, "ddc_currency_code")).withSideInputs(dimDefaultCurrencyView));
			
			
			PCollectionList<TableRow> requiredCurrencyCodeDataList = PCollectionList.of(dimDefaultCurrencyLookupJoinResult)
					.and(currencyCodeInvalidValidtuple.get(validCurrencyCodeTupleTag));

			PCollection<TableRow> requiredCurrencyCodeData = requiredCurrencyCodeDataList
					.apply("flattening valid and invalid currency record", Flatten.pCollections());
			
			
			final TupleTag<TableRow> alphaCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			final TupleTag<TableRow> numericCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			final TupleTag<TableRow> nullCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			
			PCollectionTuple currencyCodeTuple = requiredCurrencyCodeData.apply("Filtering Currency Code",
					ParDo.of(new FilterCurrencyCodeByAlphaAndNumeric(alphaCurrencyCodeTupleTag, numericCurrencyCodeTupleTag,
							nullCurrencyCodeTupleTag)).withOutputTags(alphaCurrencyCodeTupleTag,
									TupleTagList.of(numericCurrencyCodeTupleTag).and(nullCurrencyCodeTupleTag)));
			
			
			PCollection<TableRow> dimIsoNumericCurrencyCodeData = pipeline.apply("reading dim_iso_numeric_currency_code",
					BigQueryIO.readTableRows().fromQuery(CommonUtil.DIM_ISO_NUMERIC_CURRENCY_CODE_QUERY)
							.usingStandardSql().withoutResultFlattening().withoutValidation().withTemplateCompatibility());	
			
			
			PCollectionView<Map<String, Iterable<TableRow>>> alphaCurrencyCodeData = dimIsoNumericCurrencyCodeData
					.apply("Filtering And Converting To View", new FetchRequiredDataAndConvertToView("dincc_currency_code",
							Utility.getDimISoNumericCurrencyCodeColumnList()));

			PCollectionView<Map<String, Iterable<TableRow>>> numericCurrencyCodeData = dimIsoNumericCurrencyCodeData.apply(
					"Filtering And Converting To View",new FetchRequiredDataAndConvertToView("dincc_iso_numeric_currency_code",
							Utility.getDimISoNumericCurrencyCodeColumnList()));
			
			
			
			//add alpha and iso columns in below 2 joins
			PCollection<TableRow> dimAlphaCurrencyCodeSideInputJoin = currencyCodeTuple
					.get(alphaCurrencyCodeTupleTag).apply(
							"Performing Join with alpha Dim_iso_numeric_currency_code", ParDo
									.of(new FinalLeftOuterJoinSideInputCurrencyCode("updated_currency_code",
											"dincc_currency_code", alphaCurrencyCodeData,CommonUtil.ALPHA_CURRENCY_CODE,
											CommonUtil.ISO_NUMERIC_CURRENCY_CODE,CommonUtil.TRANSACTION_CURRENCY_CODE_SK
											))
									.withSideInputs(alphaCurrencyCodeData));
			
			
			
			PCollection<TableRow> dimNumericCurrencyCodeSideInputJoin = currencyCodeTuple.get(numericCurrencyCodeTupleTag)
					.apply("Performing Join with  numeric Dim_iso_numeric_currency_code",
							ParDo.of(new FinalLeftOuterJoinSideInputCurrencyCode("updated_currency_code",
									"dincc_iso_numeric_currency_code", numericCurrencyCodeData,CommonUtil.ALPHA_CURRENCY_CODE,
									CommonUtil.ISO_NUMERIC_CURRENCY_CODE,CommonUtil.TRANSACTION_CURRENCY_CODE_SK
									))
									.withSideInputs(numericCurrencyCodeData));
			
			
			PCollectionList<TableRow> updatedcurrencyCodeDataList = PCollectionList.of(dimAlphaCurrencyCodeSideInputJoin)
					.and(dimNumericCurrencyCodeSideInputJoin).and(currencyCodeTuple.get(nullCurrencyCodeTupleTag));
			
			PCollection<TableRow> updatedcurrencyCodeData = updatedcurrencyCodeDataList
					.apply("flattening updated currency record", Flatten.pCollections())
					.apply("cleaning data ", ParDo.of(new CleaningData()));
			
			TupleTag<TableRow> invalidSettlementCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			TupleTag<TableRow> validSettlementCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};

			PCollectionTuple settlementCurrencyCodeInvalidValidtuple = updatedcurrencyCodeData.apply(
					"Splitting settlement Currency Code By Invalid Valid",
					ParDo.of(new SplittingSettlementCurrencyCodeInvalidValid(validSettlementCurrencyCodeTupleTag,
							invalidSettlementCurrencyCodeTupleTag)).withOutputTags(validSettlementCurrencyCodeTupleTag,
									TupleTagList.of(invalidSettlementCurrencyCodeTupleTag)));
			
			PCollection<TableRow> settleCurrencyWithMerchantInfoSkJoinResult = settlementCurrencyCodeInvalidValidtuple
					.get(invalidSettlementCurrencyCodeTupleTag)
					.apply(" settlement with Performing Left Outer Join With dim_merchant_information", ParDo
							.of(new LeftOuterJoinSideInput("merchant_number_int", "dmi_merchant_number", dimMerchantInfoView))
							.withSideInputs(dimMerchantInfoView));

			PCollection<TableRow> settleCurrencyWithdimDefaultCurrencyLookupJoinResult = settleCurrencyWithMerchantInfoSkJoinResult
					.apply("settlement Performing Join with DIM_DEFAULT_CURRENCY",
							ParDo.of(new LeftOuterJoinSideInputCurrencyCode("dmi_corporate_region", "ddc_corporate_region",
									dimDefaultCurrencyView, "ddc_currency_code")).withSideInputs(dimDefaultCurrencyView));

			PCollectionList<TableRow> requiredSettlementCurrencyCodeDataList = PCollectionList
					.of(settleCurrencyWithdimDefaultCurrencyLookupJoinResult)
					.and(settlementCurrencyCodeInvalidValidtuple.get(validSettlementCurrencyCodeTupleTag));

			PCollection<TableRow> requiredSettlementCurrencyCodeData = requiredSettlementCurrencyCodeDataList
					.apply("flattening valid and invalid  settlement currency record", Flatten.pCollections());

			final TupleTag<TableRow> alphaSettlementCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			final TupleTag<TableRow> numericSettlementCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			final TupleTag<TableRow> nullSettlementCurrencyCodeTupleTag = new TupleTag<TableRow>() {
				private static final long serialVersionUID = 1L;
			};
			
			PCollectionTuple settlementCurrencyCodeTuple = requiredSettlementCurrencyCodeData.apply(
					"Filtering Currency Code",
					ParDo.of(new FilterSettlementCurrencyCodeByAlphaAndNumeric(alphaSettlementCurrencyCodeTupleTag,
							numericSettlementCurrencyCodeTupleTag, nullSettlementCurrencyCodeTupleTag))
							.withOutputTags(alphaSettlementCurrencyCodeTupleTag, TupleTagList
									.of(numericSettlementCurrencyCodeTupleTag).and(nullSettlementCurrencyCodeTupleTag)));
			
			
			//add alpha and iso columns in below 2 joins
			PCollection<TableRow> dimAlphaSettlementCurrencyCodeSideInputJoin = settlementCurrencyCodeTuple
					.get(alphaSettlementCurrencyCodeTupleTag).apply(
							"Performing Settle Join with alpha Dim_iso_numeric_currency_code", ParDo
									.of(new FinalSettleLeftOuterJoinSideInputCurrencyCode("updated_currency_code",
											"dincc_currency_code", alphaCurrencyCodeData,CommonUtil.SETTLE_ALPHA_CURRENCY_CODE,
											CommonUtil.SETTLE_ISO_CURRENCY_CODE))
									.withSideInputs(alphaCurrencyCodeData));

			PCollection<TableRow> dimNumericSettlementCurrencyCodeSideInputJoin = settlementCurrencyCodeTuple
					.get(numericSettlementCurrencyCodeTupleTag)
					.apply("Performing Settle Join with  numeric Dim_iso_numeric_currency_code",
							ParDo.of(new FinalSettleLeftOuterJoinSideInputCurrencyCode("updated_currency_code",
									"dincc_iso_numeric_currency_code", numericCurrencyCodeData,CommonUtil.SETTLE_ALPHA_CURRENCY_CODE,
									CommonUtil.SETTLE_ISO_CURRENCY_CODE))
									.withSideInputs(numericCurrencyCodeData));
			
			PCollectionList<TableRow> updatedSettlementcurrencyCodeDataList = PCollectionList
					.of(dimAlphaSettlementCurrencyCodeSideInputJoin).and(dimNumericSettlementCurrencyCodeSideInputJoin)
					.and(settlementCurrencyCodeTuple.get(nullSettlementCurrencyCodeTupleTag));
			
			///check if the set is important or not in below class
			PCollection<TableRow> updatedSettlementcurrencyCodeData = updatedSettlementcurrencyCodeDataList
					.apply("flattening updated currency record", Flatten.pCollections())
					.apply("cleaning data ", ParDo.of(new CleaningSettlementData()));

			
			PCollection<TableRow> transFtOutputData =updatedSettlementcurrencyCodeData.apply("Final Filter", ParDo.of(new TransFTFinalFilter()));
					
			// Loading data into LA_TRANS_FT table
			transFtOutputData.apply("Write updated result for LA_TRANS_FT table",
					BigQueryIO.writeTableRows().to(LA_TRANS_FT).withoutValidation()
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
			pipeline.run();
		}
	}
