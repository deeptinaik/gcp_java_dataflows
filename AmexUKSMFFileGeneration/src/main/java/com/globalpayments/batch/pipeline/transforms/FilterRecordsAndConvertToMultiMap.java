package com.globalpayments.batch.pipeline.transforms;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class FilterRecordsAndConvertToMultiMap extends DoFn<TableRow, KV<String, TableRow>>  
{
	private static final long serialVersionUID = 1L;
	
	
	String joinColumnName;
	List<String> requiredColumnList;
	
	public FilterRecordsAndConvertToMultiMap( String joinColumnName,
			List<String> requiredColumnList) 
	{
		
		this.joinColumnName = joinColumnName;
		this.requiredColumnList = requiredColumnList;
	}
	
	@ProcessElement
	public void processElement(ProcessContext c)
	{
		if (c.element().containsKey(joinColumnName))
		{
			TableRow tableRow = new TableRow();
			
			requiredColumnList.forEach( columnName -> {
				if (c.element().containsKey(columnName))
				{
					tableRow.set( columnName, c.element().get(columnName).toString());
				}
			});
					
			c.output(KV.of(c.element().get(joinColumnName).toString(), tableRow));
		}
	}
}