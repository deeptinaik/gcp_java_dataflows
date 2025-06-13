package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.services.bigquery.model.TableRow;

public class FilterSettlementCurrencyCodeByAlphaAndNumeric extends DoFn<TableRow, TableRow>
{
	private static final long serialVersionUID = 1L;
	
	
	TupleTag<TableRow> alphaSettlementCurrencyCodeTupleTag;
	TupleTag<TableRow> numericSettlementCurrencyCodeTupleTag; 
	TupleTag<TableRow> nullSettlementCurrencyCodeTupleTag;
	
	
	public FilterSettlementCurrencyCodeByAlphaAndNumeric( TupleTag<TableRow> alphaSettlementCurrencyCodeTupleTag,
			TupleTag<TableRow> numericSettlementCurrencyCodeTupleTag, TupleTag<TableRow> nullSettlementCurrencyCodeTupleTag)	
	{
		
		this.alphaSettlementCurrencyCodeTupleTag = alphaSettlementCurrencyCodeTupleTag;
		this.numericSettlementCurrencyCodeTupleTag = numericSettlementCurrencyCodeTupleTag;
		this.nullSettlementCurrencyCodeTupleTag = nullSettlementCurrencyCodeTupleTag;
	}
	
	@ProcessElement
	public void processElement(ProcessContext c)
	{
		
		if (c.element().get("ddc_currency_code")!= null
				&& c.element().get("ddc_currency_code").toString().matches("^[a-zA-Z]+$"))	
		{	
			c.element().set("updated_currency_code", c.element().get("ddc_currency_code").toString());
			
			c.output(alphaSettlementCurrencyCodeTupleTag,c.element());
		}
		else if (c.element().get("ddc_currency_code") != null
				&& c.element().get("ddc_currency_code").toString().matches("[0-9]+"))
		{
			
			c.element().set("updated_currency_code", c.element().get("ddc_currency_code").toString());
			c.output(numericSettlementCurrencyCodeTupleTag, c.element());
		}
		else if (c.element().get("settle_currency_code") != null 
				&& c.element().get("settle_currency_code").toString().matches("^[a-zA-Z]+$"))	
		{	
			
			c.element().set("updated_currency_code", c.element().get("settle_currency_code").toString());
			c.output(alphaSettlementCurrencyCodeTupleTag,c.element());
		}
		
		else if (c.element().get("settle_currency_code") != null 
				&& c.element().get("settle_currency_code").toString().matches("[0-9]+"))
		{
			
			c.element().set("updated_currency_code", c.element().get("settle_currency_code").toString());
			c.output(numericSettlementCurrencyCodeTupleTag, c.element());
		}
		else
		{
			
			c.output(nullSettlementCurrencyCodeTupleTag,c.element());
		}
	}
}
