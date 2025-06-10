package com.globalpayments.batch.pipeline.util;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import com.google.api.services.bigquery.model.TableRow;

public class ReadKvData implements SerializableFunction<SchemaAndRecord, KV<String, TableRow>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9117193108961091816L;

	private String[] key;

	public ReadKvData(String[] key) {
		this.key = key;
	}

	@Override
	public KV<String, TableRow> apply(SchemaAndRecord input) 
	{
		System.out.println("input "+input);
		System.out.println("getRecord "+input.getRecord());
		System.out.println("getTableSchema "+input.getTableSchema());
		
		TableRow output = BigQueryUtils.convertGenericRecordToTableRow(input.getRecord(), input.getTableSchema());
		StringBuilder keyBuilder = new StringBuilder();

		for (String key : key) 
		{
			if (output.containsKey(key) && null != output.get(key)	&& !"".equalsIgnoreCase(output.get(key).toString().trim()))
				
				keyBuilder.append(output.get(key).toString().trim());
		}
		System.out.println("KV"+ KV.of(keyBuilder.toString(), output));
		return KV.of(keyBuilder.toString(), output);
	}

}
