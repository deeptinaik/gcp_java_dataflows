package com.globalpayments.batch.pipeline.transforms;


import com.globalpayments.batch.pipeline.utils.Constants;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class FileGeneration extends DoFn<String, String> implements Serializable {
    private static final long serialVersionUID = -3317717067928751443L;

    PCollectionView<Map<String, Iterable<String>>> todaysDataMapView;
    PCollectionView<Long> recordCountView;
    PCollectionView<Map<String, Iterable<String>>> pendingDataMapView;
    ValueProvider<String> pendingRecordCount;
    TupleTag<String> todaysFinalDataTuple;
    TupleTag<String> pendingFinalDataTuple;
    PCollectionView<List<String>> pendingForNextDayView;


    public FileGeneration(PCollectionView<Map<String, Iterable<String>>> todaysDataMapView,
                          PCollectionView<Long> recordCountView, PCollectionView<Map<String, Iterable<String>>> pendingDataMapView,
                          ValueProvider<String> pendingRecordCount, TupleTag<String> todaysFinalDataTuple,
                          TupleTag<String> pendingFinalDataTuple, PCollectionView<List<String>> pendingForNextDayView ) {

        this.todaysDataMapView = todaysDataMapView;
        this.recordCountView = recordCountView;
        this.pendingDataMapView = pendingDataMapView;
        this.pendingRecordCount = pendingRecordCount;
        this.todaysFinalDataTuple = todaysFinalDataTuple;
        this.pendingFinalDataTuple = pendingFinalDataTuple;
        this.pendingForNextDayView = pendingForNextDayView;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        List<String> finalData = new ArrayList<>();
        List<String> pendingData = new ArrayList<>();

        Map<String, Iterable<String>> pendingDataMap = context.sideInput(pendingDataMapView);

        int recLimit = 250000;
        int dt2RecordCounter = 1;
        int dtlRecordCounter = 1;

        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .replaceAll("[-,:,' ']", "");

        if (!pendingRecordCount.toString().trim().equals("0") || context.sideInput(recordCountView) != 0 ) {

            // Sort data
            LinkedHashMap<String, Iterable<String>> sortedData = context.sideInput(todaysDataMapView).entrySet()
                    .stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

            // Pending data
            for (String key : pendingDataMap.keySet()) {

                if (dt2RecordCounter <= recLimit && dtlRecordCounter <= recLimit + 1) {

                    int recordCounter = 1;
                    StringBuilder header = new StringBuilder();

                        header.append(Constants.NO_DATA_HEADER_PART_1).append(StringUtils.leftPad("", 38, " "))
                                .append(Constants.NO_DATA_HEADER_PART_2).append(StringUtils.leftPad("", 25, " "))
                                .append(key.substring(0, 10)).append(Constants.NO_DATA_HEADER_PART_3).append(currentTime)
                                .append(currentTime).append(StringUtils.leftPad("", 2612, " "));

                    finalData.add(header.toString());

                    SortedMap<String, String> sortedMapAll = new TreeMap<>();
                    SortedMap<String, String> sortedMap = new TreeMap<>();

                    // Put all pending data into sortedMap
                    for (String row : pendingDataMap.get(key)) {

                        if (row.substring(10, 13).equals("DTL")) {
                            sortedMapAll.put(row.substring(13, 29).concat("1"), row);
                        } else {
                            sortedMapAll.put(row.substring(13, 29).concat("2"), row);
                        }
                    }

                    // Iterate over sorted Map
                    for (String s : sortedMapAll.keySet()) {
                        if (dt2RecordCounter <= recLimit && dtlRecordCounter <= recLimit + 1) {

                            if (sortedMapAll.get(s).substring(10, 13).equals("DTL")) {

                                sortedMap.put(sortedMapAll.get(s).substring(13, 29).concat("1"), sortedMapAll.get(s));
                                dtlRecordCounter++;
                            } else {
                                sortedMap.put(sortedMapAll.get(s).substring(13, 29).concat("2"), sortedMapAll.get(s));
                                dt2RecordCounter++;
                            }
                        }

                        else {
                            StringBuilder pendingDataRow = new StringBuilder();
                            pendingDataRow.append(key).append(",").append(sortedMapAll.get(s));
                            pendingData.add(pendingDataRow.toString());
                        }
                    }

                    for (String s : sortedMap.keySet()) {
                        StringBuilder record = new StringBuilder();
                        int len = sortedMap.get(s).length();
                        record.append(sortedMap.get(s).substring(10, 13))
                                .append(StringUtils.leftPad((Integer.toString(++recordCounter)), 8, "0"))
                                .append(sortedMap.get(s).substring(13,len-8));
                        finalData.add(record.toString());

                    }
                    StringBuilder trailer = new StringBuilder();
                    trailer.append("TLR").append(StringUtils.leftPad(Integer.toString((++recordCounter)), 8, "0"))
                            .append(StringUtils.leftPad(Integer.toString((recordCounter)), 9, "0"))
                            .append(StringUtils.leftPad("", 2731, " "));

                    finalData.add(trailer.toString());
                }

                else {

                    for (String row : pendingDataMap.get(key)) {
                        StringBuilder pendingDataRow = new StringBuilder();
                        pendingDataRow.append(key).append(",").append(row);
                        pendingData.add(pendingDataRow.toString());
                    }
                }
            }

            // today's data
            for (String key : sortedData.keySet()) {
                int recordCounter = 1;

                if (dt2RecordCounter <= recLimit && dtlRecordCounter <= recLimit + 1) {

                    StringBuilder header = new StringBuilder();


                        header.append(Constants.NO_DATA_HEADER_PART_1).append(StringUtils.leftPad("", 38, " "))
                                .append(Constants.NO_DATA_HEADER_PART_2).append(StringUtils.leftPad("", 25, " "))
                                .append(key.substring(0, 10)).append(Constants.NO_DATA_HEADER_PART_3).append(currentTime)
                                .append(currentTime).append(StringUtils.leftPad("", 2612, " "));
                    finalData.add(header.toString());

                    SortedMap<String, String> sortedMapAll = new TreeMap<>();
                    SortedMap<String, String> sortedMap = new TreeMap<>();

                    // Put today's data into sortedMap
                    for (String row : sortedData.get(key)) {
                        if (row.substring(10, 13).equals("DTL")) {
                            sortedMapAll.put(row.substring(13, 29).concat("1"), row);
                        } else {
                            sortedMapAll.put(row.substring(13, 29).concat("2"), row);
                        }
                    }
                    // Iterate over sorted Map
                    for (String s : sortedMapAll.keySet()) {
                        if (dt2RecordCounter <= recLimit && dtlRecordCounter <= recLimit + 1) {

                            if (sortedMapAll.get(s).substring(10, 13).equals("DTL")) {
                                sortedMap.put(sortedMapAll.get(s).substring(13, 29).concat("1"), sortedMapAll.get(s));
                                dtlRecordCounter++;
                            } else {
                                sortedMap.put(sortedMapAll.get(s).substring(13, 29).concat("2"), sortedMapAll.get(s));
                                dt2RecordCounter++;
                            }

                        }

                        else {
                            StringBuilder pendingDataRow = new StringBuilder();
                            pendingDataRow.append(key).append(",").append(sortedMapAll.get(s));
                            pendingData.add(pendingDataRow.toString());
                        }
                    }

                    for (String s : sortedMap.keySet()) {
                        StringBuilder record = new StringBuilder();
                        int len = sortedMap.get(s).length();
                        record.append(sortedMap.get(s).substring(10, 13))
                                .append(StringUtils.leftPad((Integer.toString(++recordCounter)), 8, "0"))
                                .append(sortedMap.get(s).substring(13,len-8));
                        finalData.add(record.toString());

                    }

                    StringBuilder trailer = new StringBuilder();
                    trailer.append("TLR").append(StringUtils.leftPad(Integer.toString((++recordCounter)), 8, "0"))
                            .append(StringUtils.leftPad(Integer.toString((recordCounter)), 9, "0"))
                            .append(StringUtils.leftPad("", 2731, " "));

                    finalData.add(trailer.toString());

                }

                else {

                    for (String row : sortedData.get(key)) {
                        StringBuilder pendingDataRow = new StringBuilder();
                        pendingDataRow.append(key).append(",").append(row);
                        pendingData.add(pendingDataRow.toString());
                    }
                }

            }
            if (!context.sideInput(pendingForNextDayView).isEmpty()) {
                Iterator<String> itr = context.sideInput(pendingForNextDayView).iterator();
                while(itr.hasNext()) {
                    pendingData.add(itr.next());
                }
            }

        }

        else

        {
            StringBuilder header = new StringBuilder();
            header.append(Constants.NO_DATA_HEADER_PART_1).append(StringUtils.leftPad("", 38, " "))
                    .append(Constants.NO_DATA_HEADER_PART_2).append(StringUtils.leftPad("", 25, " ")).append("7490833209")
                    .append(Constants.NO_DATA_HEADER_PART_3).append(currentTime).append(currentTime)
                    .append(StringUtils.leftPad("", 2612, " "));
            finalData.add(header.toString());
            StringBuilder trailer = new StringBuilder();
            trailer.append("TLR").append("00000002").append("000000002").append(StringUtils.leftPad("", 2731, " "));
            finalData.add(trailer.toString());
        }

        context.output(todaysFinalDataTuple, String.join("\n", finalData));

        if (pendingData.isEmpty()) {
            context.output(pendingFinalDataTuple, "");
        } else {
            context.output(pendingFinalDataTuple, String.join("\n", pendingData));
        }
    }
}
