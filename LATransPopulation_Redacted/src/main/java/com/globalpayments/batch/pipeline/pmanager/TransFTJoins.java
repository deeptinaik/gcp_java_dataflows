package com.globalpayments.batch.pipeline.pmanager;

import java.util.Map;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.globalpayments.batch.pipeline.transforms.DimMerchantInfoLookUp;
import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.globalpayments.batch.pipeline.util.Utility;
import com.google.api.services.bigquery.model.TableRow;

public class TransFTJoins implements CommonUtil
{
	
	/**
	 * Processing lookup with DimMerchantInfo table.
	 * 
	 * @return PCollection of table join result
	 */
	public static PCollection<TableRow> joinMerchantInformationDetails(PCollection<TableRow> sourceTable,
			PCollection<KV<String, TableRow>> dimMerchantInformation) {

		PCollectionView<Map<String, TableRow>> dimMerchantView = Utility.convertTableRowInToMap(dimMerchantInformation);

		return sourceTable.apply("Dim_Merchant_Info lookup",
				ParDo.of(new DimMerchantInfoLookUp(dimMerchantView)).withSideInputs(dimMerchantView));
	}

}
