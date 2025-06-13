package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class SplitElements extends DoFn<String, Long> {

    private static final long serialVersionUID = -7876284218456116961L;

    @ProcessElement
    public void processElement(ProcessContext context) {

        String[] elements = context.element().split("\n");

        Long len = (long) elements.length;
        if (len == 1 && elements[0].equals("")) {

            len = (long) 0;

            context.output(len);
        } else {
            context.output(len);
        }

    }

}