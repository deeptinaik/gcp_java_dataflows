package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class FinalLeftOuterJoinSideInputCurrencyCode extends DoFn<TableRow, TableRow>
{
	private static final long serialVersionUID = 1L;
	
	String leftKey;
	String rightKey;
	String sourceColumnName;
	PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView;
	String alphaData;
	String isoData;
	String isoNumericCurrencyCodeSk;
	
	public FinalLeftOuterJoinSideInputCurrencyCode(String leftKey, String rightKey,
			PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView,String alphaData, 
			String isoData, String isoNumericCurrencyCodeSk
			) 
	{
		this.leftKey = leftKey;
		this.rightKey = rightKey;
		this.rightTableMultiMapView = rightTableMultiMapView;
		this.alphaData = alphaData;
		this.isoData = isoData;
		this.isoNumericCurrencyCodeSk=isoNumericCurrencyCodeSk;
	}

	@ProcessElement
	public void processElement(ProcessContext c)
	{
		Map<String, Iterable<TableRow>> rightTableMultiMap = c.sideInput(rightTableMultiMapView); 		
		
		if (c.element().containsKey(leftKey) && !c.element().get(leftKey).toString().trim().isEmpty()
				&& rightTableMultiMap.containsKey(c.element().get(leftKey).toString().trim()))		
		{
			for(TableRow tblRow : rightTableMultiMap.get(c.element().get(leftKey).toString().trim()))
			{
				if(tblRow.containsKey(rightKey))
				{
					
					c.element().set(alphaData, tblRow.get("dincc_currency_code").toString());
					c.element().set(isoData, tblRow.get("dincc_iso_numeric_currency_code").toString());
					c.element().set(isoNumericCurrencyCodeSk, tblRow.get("dincc_iso_numeric_currency_code_sk").toString());
				}
				c.output(c.element());
			}
		}
		else
		{
			c.output(c.element());
		}
	}
}
