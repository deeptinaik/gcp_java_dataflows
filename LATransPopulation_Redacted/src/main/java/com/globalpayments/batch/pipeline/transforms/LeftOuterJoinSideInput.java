package com.globalpayments.batch.pipeline.transforms;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class LeftOuterJoinSideInput extends  DoFn<TableRow, TableRow>
{
	private static final long serialVersionUID = 1L;
	
	String leftKey;
	String rightKey;
	String sourceColumnName;
	PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView;
	
	public LeftOuterJoinSideInput(String leftKey, String rightKey, 
			PCollectionView<Map<String, Iterable<TableRow>>> rightTableMultiMapView) 
	{
		this.leftKey = leftKey;
		this.rightKey = rightKey;
		this.rightTableMultiMapView = rightTableMultiMapView;
	}

	@ProcessElement
	public void processElement(ProcessContext c)
	{
		Map<String, Iterable<TableRow>> rightTableMultiMap = c.sideInput(rightTableMultiMapView); 		
		
		if (c.element().containsKey(leftKey) && c.element().get(leftKey) !=null &&!c.element().get(leftKey).toString().trim().isEmpty()
				&& rightTableMultiMap.containsKey(c.element().get(leftKey).toString().trim()))		
		{
			for(TableRow tblRow : rightTableMultiMap.get(c.element().get(leftKey).toString().trim()))
			{
				if(tblRow.containsKey(rightKey))
				{
					String corporate_region = tblRow.get( "dmi_corporate").toString().concat(tblRow.get("dmi_region").toString());
					c.element().set("dmi_corporate_region", corporate_region);
					
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
