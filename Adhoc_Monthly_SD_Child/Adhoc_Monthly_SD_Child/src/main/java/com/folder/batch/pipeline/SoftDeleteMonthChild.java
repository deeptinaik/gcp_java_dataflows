package com.folder.batch.pipeline;

/**
* SoftDeleteMonthlyChild is a controller class, this class starts the processing of Soft delete logic applying to child tables. 
* It reads TableDescription (Master table) and ChildTableDescription(child table) from AirFlow options.
* @author  Bitwise Offshore
* @version 1.0
* 
*/

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.folder.airflow.AirflowOptions;
import com.folder.batch.pipeline.pmanager.ChildSDMonthly;
import com.google.api.services.bigquery.model.TableRow;

public class SoftDeleteMonthChild {

	public static final Logger logger = LoggerFactory.getLogger(SoftDeleteMonthChild.class);

	public static void main(String[] args){

		AirflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AirflowOptions.class);
		//Uncomment below code when runing this template locally.
		//options.setTempDatasetId("bigquery.googleapis.com/Dev");
		
		logger.info("Adhoc monthly child soft delete process started.");
		
		Pipeline dataFlowPipeline = Pipeline.create(options);
		PCollection<TableRow> finalCollection = dataFlowPipeline.apply("Apply Soft Delete on child table",
				new ChildSDMonthly(options));
		
		finalCollection.apply("Write final result to BigQuery",
				BigQueryIO.writeTableRows().to(options.getChildTableDescription())
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		dataFlowPipeline.run();
	}	
}