package com.globalpayments.batch.pipeline.pmanager;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globalpayments.batch.pipeline.transforms.TempTransFT1Validation;
import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.TableFieldFilter;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TransFTProcessing extends PTransform<PBegin, PCollection<TableRow>> implements CommonUtil
{
	private static final long serialVersionUID = -8593175381756824559L;
	public static final Logger logger = LoggerFactory.getLogger(TransFTProcessing.class);

	PCollection<TableRow> intakeTable; // NOSONAR
	String[] merchantInformationDetailsFields = { MERCHANT_NUMBER, HIERARCHY, CORPORATE, REGION,MERCHANT_INFORMATION_SK };
	String[] isoNumericFields = { ISO_NUMERIC_CURRENCY_CODE, CURRENCY_CODE, ISO_NUMERIC_CURRENCY_CODE_SK };
	String[] avsResponseCodeDmFields = { AVS_RESPONSE_CODE, CARD_SCHEME_SK, AVS_RESPONSE_CODE_SK };
	String[] merchantInformationDmKey = { MERCHANT_NUMBER };
	String[] avsResponseCodeDmKey = { AVS_RESPONSE_CODE, CARD_SCHEME_SK };

	public TransFTProcessing(PCollection<TableRow> intakeTable) {
		this.intakeTable = intakeTable;
	}
	
	@Override
	public PCollection<TableRow> expand(PBegin pBegin) {

		// Fetching dim_merchant_information table data from BigQuerytables
		PCollection<KV<String, TableRow>> dimMerchantInformation = Utility.fetchTableRowsUsingQuery(pBegin,
				DIM_MERCHANT_INFORMATION_QUERY, "Reading from merchantInformationDetails Table",
				merchantInformationDmKey);

		// Dim_Merchant_Information join
				PCollection<TableRow> merchantInfoJoin = TransFTJoins.joinMerchantInformationDetails(intakeTable, dimMerchantInformation);
		
				
				
		// Calculate TEMP_TRANS_FT0 result
				PCollection<TableRow> tempTransFT0 = pBegin.apply("TempTransFT0 result",new TempTransFT0Processing(merchantInfoJoin));

					
		
		// Validation of final result values
				return tempTransFT0.apply("Transft validation", ParDo.of(new TempTransFT1Validation()));
		
		
	}
}
