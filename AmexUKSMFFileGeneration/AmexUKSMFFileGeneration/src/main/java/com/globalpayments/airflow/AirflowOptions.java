package com.globalpayments.airflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;


    public interface AirflowOptions extends DataflowPipelineOptions, DataflowWorkerHarnessOptions {

        @Description("$etlbatchid required to fetch the data from the Source")
        public void setEtlbatchid(ValueProvider<String> etlbatchid);
        public ValueProvider<String> getEtlbatchid();

        //source pending file
        @Description("Pending data file")
        ValueProvider<String> getPendingDataFile();
        void setPendingDataFile(ValueProvider<String> srcPendingFile);

        //Records pending
        @Description("Pending data file")
        ValueProvider<String> getPendingRecordCount();
        void setPendingRecordCount(ValueProvider<String> pendingRecordCount);

        @Description("output file path")
        ValueProvider<String> getOutputFilePath();
        void setOutputFilePath(ValueProvider<String> outputFilePath);

        //target
        @Description("Target Pending data file")
        ValueProvider<String> getTargetPendingDataFile();
        void setTargetPendingDataFile(ValueProvider<String> tgtPendingFile);

        @Description("CloudSql Info path")
        ValueProvider<String> getConfigPath();
        void setConfigPath(ValueProvider<String> configPath);

    }

