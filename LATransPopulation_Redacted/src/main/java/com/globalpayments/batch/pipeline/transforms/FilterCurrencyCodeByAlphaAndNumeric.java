package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.TupleTag;

import com.google.api.services.bigquery.model.TableRow;

public class FilterCurrencyCodeByAlphaAndNumeric extends  DoFn<TableRow, TableRow>
{
	private static final long serialVersionUID = 1L;
	
	
	TupleTag<TableRow> alphaCurrencyCodeTupleTag;
	TupleTag<TableRow> numericCurrencyCodeTupleTag; 
	TupleTag<TableRow> nullCurrencyCodeTupleTag;
	
	public FilterCurrencyCodeByAlphaAndNumeric( TupleTag<TableRow> alphaCurrencyCodeTupleTag,
			TupleTag<TableRow> numericCurrencyCodeTupleTag, TupleTag<TableRow> nullCurrencyCodeTupleTag)	
	{
		
		this.alphaCurrencyCodeTupleTag = alphaCurrencyCodeTupleTag;
		this.numericCurrencyCodeTupleTag = numericCurrencyCodeTupleTag;
		this.nullCurrencyCodeTupleTag = nullCurrencyCodeTupleTag;
	}
	
	@ProcessElement
	public void processElement(ProcessContext c)
	{
		
		
		if (c.element().get("ddc_currency_code")!= null && c.element().get("ddc_currency_code").toString().matches("^[a-zA-Z]+$"))	
		{	
			c.element().set("updated_currency_code", c.element().get("ddc_currency_code").toString());			
			c.output(alphaCurrencyCodeTupleTag,c.element());
		}
		else if (c.element().get("ddc_currency_code") != null && c.element().get("ddc_currency_code").toString().matches("[0-9]+"))
		{
			
			c.element().set("updated_currency_code", c.element().get("ddc_currency_code").toString());
			c.output(numericCurrencyCodeTupleTag, c.element());
		}
		
		
		//
		else if (c.element().get("transaction_currency_code") != null 
				&& c.element().get("transaction_currency_code").toString().matches("^[a-zA-Z]+$"))	
		{	
			
			c.element().set("updated_currency_code", c.element().get("transaction_currency_code").toString());
			c.output(alphaCurrencyCodeTupleTag,c.element());

		}
		
		else if (c.element().get("transaction_currency_code") != null 
				&& c.element().get("transaction_currency_code").toString().matches("[0-9]+"))
		{
			
			c.element().set("updated_currency_code", c.element().get("transaction_currency_code").toString());
			c.output(numericCurrencyCodeTupleTag, c.element());
		}
		else
		{
			
			c.output(nullCurrencyCodeTupleTag,c.element());
		}
	}
}
