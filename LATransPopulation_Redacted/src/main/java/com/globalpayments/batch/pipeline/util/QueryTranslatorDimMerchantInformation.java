package com.globalpayments.batch.pipeline.util;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class QueryTranslatorDimMerchantInformation implements SerializableFunction<String, String> {

	/*ValueProvider<String> etlbatchid;

	public QueryTranslatorDimMerchantInformation() 
	{
		this.etlbatchid = etlbatchid;
	}*/

	private static final long serialVersionUID = -2754362391392873056L;

	@Override
	public String apply(String input) 
	{

		return String.format(
				"select  merchant_number as dmi_merchant_number, corporate as dmi_corporate, region as dmi_region from `%s` "
				+ "where current_ind = '0' and merchant_number in (select  ltrim(merchant_number,'0') from `%s` where etlbatchid = '%s')",
				"transformed_layer.dim_merch_info","trusted_layer.la_trans_ft",input);
	}
}
