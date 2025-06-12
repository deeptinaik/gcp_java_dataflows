package com.globalpayments.batch.pipeline.transforms;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ConvertToKV extends DoFn<String, KV<String, String>> {

    private static final long serialVersionUID = 7967036052848965317L;

    @ProcessElement
    public void processElement(ProcessContext c) {

        String[] elements = c.element().split(",", 2);
        if(elements.length == 2) {
            c.output(KV.of(elements[0], elements[1]));
        }


    }
}