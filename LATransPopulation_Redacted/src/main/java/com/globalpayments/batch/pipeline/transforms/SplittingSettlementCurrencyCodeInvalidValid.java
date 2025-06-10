package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.services.bigquery.model.TableRow;

public class SplittingSettlementCurrencyCodeInvalidValid extends DoFn<TableRow, TableRow> {
	private static final long serialVersionUID = 1L;

	TupleTag<TableRow> invalidSettlementCurrencyCodeTupleTag;
	TupleTag<TableRow> validSettlementCurrencyCodeTupleTag;
	

	public SplittingSettlementCurrencyCodeInvalidValid(TupleTag<TableRow> validSettlementCurrencyCodeTupleTag,
			TupleTag<TableRow> invalidSettlementCurrencyCodeTupleTag) {

		this.invalidSettlementCurrencyCodeTupleTag = invalidSettlementCurrencyCodeTupleTag;
		this.validSettlementCurrencyCodeTupleTag = validSettlementCurrencyCodeTupleTag;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		
		

		if (c.element().get("settle_currency_code")!= null
				&& !c.element().get("settle_currency_code").toString().trim().equals("")
				&& !c.element().get("settle_currency_code").toString().trim().equals("000")) {
			
			c.output(validSettlementCurrencyCodeTupleTag,c.element());

		} else {
			
			c.output(invalidSettlementCurrencyCodeTupleTag, c.element());
		}
	}
}

