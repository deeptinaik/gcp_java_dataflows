package com.globalpayments.batch.pipeline.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class TableFieldFilter extends DoFn<TableRow, TableRow> implements CommonUtil {
	private static final long serialVersionUID = -3995666265798761858L;
	public static final Logger logger = LoggerFactory.getLogger(TableFieldFilter.class);

	String[] fields;

	public TableFieldFilter(String[] fields) {
		this.fields = fields;
	}

	@ProcessElement
	public void processElement(ProcessContext processContext) {
		TableRow outputTableRow = new TableRow();

		if (null != processContext.element().get(CURRENT_IND)
				&& processContext.element().get(CURRENT_IND).toString().trim().equalsIgnoreCase(ZERO)) {
			for (String field : fields) {
				if (processContext.element().containsKey(field)) {
					outputTableRow.set(field, processContext.element().get(field));
				}
			}
			processContext.output(outputTableRow);
		}
	}
}
