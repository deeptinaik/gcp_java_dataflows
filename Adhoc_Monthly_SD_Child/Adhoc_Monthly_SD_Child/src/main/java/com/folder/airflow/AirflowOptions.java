package com.globalpayments.airflow;

/**
 * This interface used to declare value providers i.e. value received from Airflow.
 * @Author: Bitwise Offshore
 * 
 * */

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;


public interface AirflowOptions extends DataflowPipelineOptions, DataflowWorkerHarnessOptions{
	
 
    
    @Description("Child Table Location")
    @Required
	ValueProvider<String> getChildTableDescription();
    void setChildTableDescription(ValueProvider<String> childTableDescription);
    

}
