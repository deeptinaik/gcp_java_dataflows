package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollectionView;

import com.globalpayments.batch.pipeline.util.CommonUtil;
import com.google.api.services.bigquery.model.TableRow;

public class DimMerchantInfoLookUp extends DoFn<TableRow, TableRow> implements CommonUtil {
	private static final long serialVersionUID = 8037436660492490911L;

	PCollectionView<Map<String, TableRow>> dimMerchantInfoView;

	public DimMerchantInfoLookUp(PCollectionView<Map<String, TableRow>> dimMerchantInfoView) {
		this.dimMerchantInfoView = dimMerchantInfoView;
	}

	@ProcessElement
	public void processElement(ProcessContext processContext) 
	{
		Map<String, TableRow> dimMerchantInfoMap = processContext.sideInput(dimMerchantInfoView);
		TableRow outputTableRow = processContext.element().clone();

		TableRow dimMerchantInfo = dimMerchantInfoMap.get(processContext.element().get(MERCHANT_NUMBER_INT));

		if (null != dimMerchantInfo) 
		{
			outputTableRow.set(MHD_CORPORATE,null != dimMerchantInfo.get(CORPORATE) ? dimMerchantInfo.get(CORPORATE) : BLANK);
			outputTableRow.set(MHD_REGION, null != dimMerchantInfo.get(REGION) ? dimMerchantInfo.get(REGION) : BLANK);
			outputTableRow.set(MHD_HIERARCHY ,null != dimMerchantInfo.get(HIERARCHY) ? dimMerchantInfo.get(HIERARCHY) : BLANK);

			if (null != dimMerchantInfo.get(MERCHANT_INFORMATION_SK))
				outputTableRow.set(MERCHANT_INFORMATION_SK, dimMerchantInfo.get(MERCHANT_INFORMATION_SK));
		}
		System.out.println("MERCHANT_INFORMATION_SK "+outputTableRow);
		processContext.output(outputTableRow);
	}
}
