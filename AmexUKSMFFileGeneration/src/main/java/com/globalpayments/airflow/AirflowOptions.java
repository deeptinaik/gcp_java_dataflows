package com.globalpayments.airflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface AirflowOptions extends DataflowPipelineOptions, DataflowWorkerHarnessOptions{
	@Description("Job Batch Id of the whole flow")
	ValueProvider<String> getJobBatchId();
    void setJobBatchId(ValueProvider<String> jobBatchId);
    
}