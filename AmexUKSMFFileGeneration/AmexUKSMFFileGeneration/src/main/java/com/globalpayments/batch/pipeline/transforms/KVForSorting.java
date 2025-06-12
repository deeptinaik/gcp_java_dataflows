package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KVForSorting extends DoFn<String, KV<String, String>> {

    private static final long serialVersionUID = 7967036052848965317L;

    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(KV.of(c.element().substring(0, 10), c.element()));

    }
}