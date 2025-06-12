package com.globalpayments.batch.pipeline.transforms;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SplitAndConvertToKV extends DoFn <String, KV<String, String>> {

    /**
     *
     */
    private static final long serialVersionUID = -7638884593958065512L;
    public static final Logger log = LoggerFactory.getLogger(ConvertToKV.class);

    @ProcessElement
    public void processElement(ProcessContext c) {

        String[] elements = c.element().split("\n");

        for(int i=0;i<elements.length;i++) {
            String[] element = elements[i].split(",", 2);

            if(element.length == 2) {
                c.output(KV.of(element[0], element[1]));
            }
        }


    }
}