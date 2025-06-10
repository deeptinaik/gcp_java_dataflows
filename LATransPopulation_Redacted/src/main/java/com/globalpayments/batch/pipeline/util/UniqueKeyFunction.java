package com.globalpayments.batch.pipeline.util;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class UniqueKeyFunction implements SerializableFunction<KV<String, TableRow>, String>{

	/**
	 * Helper for Distinct transformation
	 */
	private static final long serialVersionUID = 2876814842300683687L;

	@Override
	public String apply(KV<String, TableRow> input) {
		return input.getKey();
	}
}