package com.globalpayments.batch.pipeline.utils;

import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.regex.Pattern;

public class QueryTranslator implements SerializableFunction<String, String> {

    private static final long serialVersionUID = -4810241463696192175L;
    private String amexUKBaseQuery;

    public QueryTranslator(String amexUKBaseQuery) {
        this.amexUKBaseQuery = amexUKBaseQuery;
    }

    @Override
    public String apply(String eltbatchid) {
        return (amexUKBaseQuery.replaceAll(Pattern.quote("$etlbatchid"), eltbatchid));
    }
}
