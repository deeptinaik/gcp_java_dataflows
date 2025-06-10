package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class LeftOuterJoinSideInputCurrencyCode extends DoFn<TableRow, TableRow>
{
	private static final long serialVersionUID = 1L;
	
	String leftKey;
	String rightKey;
	String targetColumnName;
	PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView;
	
	public LeftOuterJoinSideInputCurrencyCode(String leftKey, String rightKey,
			PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView,String targetColumnName) 
	{
		this.leftKey = leftKey;
		this.rightKey = rightKey;
		this.rightTableMultiMapView = rightTableMultiMapView;
		this.targetColumnName = targetColumnName;
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
				if(tblRow.containsKey(rightKey)&& tblRow.containsKey(targetColumnName))
				{
					
					c.element().set(targetColumnName, tblRow.get(targetColumnName).toString());
					
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
