package com.globalpayments.batch.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;
import java.util.ListIterator;

public class ConvertTableRowToString extends DoFn<TableRow, String>{

    private static final long serialVersionUID = -8007816227200473220L;
    private List<String> columnList;

    public ConvertTableRowToString(List<String> columnList) {
        this.columnList = columnList;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        StringBuilder  finalStringBuilder = new StringBuilder();
        TableRow sourceTableRow = c.element();
        ListIterator<String> iterator = columnList.listIterator();
        while (iterator.hasNext()) {
            String columnName = iterator.next();
            finalStringBuilder.append(sourceTableRow.get(columnName));
        }
        c.output(finalStringBuilder.toString());

    }
}
