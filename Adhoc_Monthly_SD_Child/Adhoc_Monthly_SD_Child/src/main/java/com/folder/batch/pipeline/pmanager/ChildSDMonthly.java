package com.folder.batch.pipeline.pmanager;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.folder.airflow.AirflowOptions;
import com.folder.batch.pipeline.transforms.CommonTimestamp;
import com.folder.batch.pipeline.transforms.FullRefresh;
import com.google.api.services.bigquery.model.TableRow;


public class ChildSDMonthly extends PTransform<PBegin, PCollection<TableRow>> {
	
	private static final long serialVersionUID = -5055324511089240877L;
	
	public static final Logger logger = LoggerFactory.getLogger(ChildSDMonthly.class);
	
	
	AirflowOptions options;
	
	public ChildSDMonthly(AirflowOptions options) {
		this.options = options;
	}
	
	@Override
	public PCollection<TableRow> expand(PBegin pBegin)   {
		
		
		//Calculate the Current Timestamp to update DW_UPD_DTM field
		PCollectionView<String> dateTimeNow = pBegin.apply("Create common timestamp value",
				Create.of("common timestamp value")).apply("Common Timestamp", ParDo.of(new CommonTimestamp()))
				.apply(View.asSingleton());
		

		//Returning the all child table records.
		return pBegin.apply("Reading the Child Table: ",
				BigQueryIO.readTableRows().from(options.getChildTableDescription()).withoutValidation().withTemplateCompatibility())
				.apply("filetering tablerows, full refersh" ,
						ParDo.of(new FullRefresh(dateTimeNow)).withSideInputs(dateTimeNow));
				
				
		
		

	}
}



