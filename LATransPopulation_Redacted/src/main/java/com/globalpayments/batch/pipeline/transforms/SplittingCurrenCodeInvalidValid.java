package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.services.bigquery.model.TableRow;

public class SplittingCurrenCodeInvalidValid extends DoFn<TableRow, TableRow> {
	private static final long serialVersionUID = 1L;

	TupleTag<TableRow> invalidCurrencyCodeTupleTag;
	TupleTag<TableRow> validCurrencyCodeTupleTag;
	

	public SplittingCurrenCodeInvalidValid(TupleTag<TableRow> validCurrencyCodeTupleTag,
			TupleTag<TableRow> invalidCurrencyCodeTupleTag) {

		this.invalidCurrencyCodeTupleTag = invalidCurrencyCodeTupleTag;
		this.validCurrencyCodeTupleTag = validCurrencyCodeTupleTag;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		
	
		if (c.element().containsKey("transaction_currency_code") && c.element().get("transaction_currency_code") != null
				&& !c.element().get("transaction_currency_code").toString().trim().equals("")
				&& !c.element().get("transaction_currency_code").toString().trim().equals("000")) {
			
			c.output(c.element());

		} else {
			
			c.output(invalidCurrencyCodeTupleTag, c.element());
		}
	}
}
