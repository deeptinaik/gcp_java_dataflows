package com.globalpayments.batch.pipeline.transforms;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class FetchRequiredDataAndConvertToView extends PTransform<PCollection<TableRow>, PCollectionView<Map<String, Iterable<TableRow>>>> 
{
	private static final long serialVersionUID = 1L;
	
	String joinColumnName;
	List<String> requiredColumnList;
	
	public FetchRequiredDataAndConvertToView( String joinColumnName,
			List<String> requiredColumnList) 
	{
		
		this.joinColumnName = joinColumnName;
		this.requiredColumnList = requiredColumnList;
	}
	
	@Override
	public PCollectionView<Map<String, Iterable<TableRow>>> expand(PCollection<TableRow> input) 
	{
		return input
				.apply("Filtering Null Records And Converting To Multimap",
						ParDo.of(new FilterRecordsAndConvertToMultiMap( joinColumnName, requiredColumnList)))
				.apply("Converting To View", View.asMultimap());
	}

}