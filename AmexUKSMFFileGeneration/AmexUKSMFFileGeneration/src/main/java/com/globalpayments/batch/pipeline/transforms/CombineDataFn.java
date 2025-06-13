package com.globalpayments.batch.pipeline.transforms;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/*
 * @Author Bitwise
 * @Date: 2024-09-27
 * @Description: Sorting of final records based on CAP+Optblue_se_number, merchant_number ,status,update date and record_type
 * updated code to create file with 250000 limit of merchants and extra records would go into pending data file which will be processed next day
 *
 * */

public class CombineDataFn extends DoFn<String, String> {

    private static final long serialVersionUID = -8485411119309286141L;
    PCollectionView<Map<String, Iterable<String>>> targetPCollectionView;
    PCollectionView<Long> recordCountView;
    PCollectionView<Map<String, Iterable<String>>> pendingDataMapView;
    ValueProvider<String> pendingRecordCount;
    TupleTag<String> todaysDataTuple;
    TupleTag<String> pendingDataTuple;
    TupleTag<String> pendingForNextDayTuple;

    public CombineDataFn(PCollectionView<Map<String, Iterable<String>>> targetPCollectionView,
                         PCollectionView<Long> recordCountView, PCollectionView<Map<String, Iterable<String>>> pendingDataMapView,
                         ValueProvider<String> pendingRecordCount, TupleTag<String> todaysDataTuple,
                         TupleTag<String> pendingDataTuple,TupleTag<String> pendingForNextDayTuple) {
        this.targetPCollectionView = targetPCollectionView;
        this.recordCountView = recordCountView;
        this.pendingDataMapView = pendingDataMapView;
        this.pendingRecordCount = pendingRecordCount;
        this.todaysDataTuple = todaysDataTuple;
        this.pendingDataTuple = pendingDataTuple;
        this.pendingForNextDayTuple = pendingForNextDayTuple;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        List<String> todaysDataList = new ArrayList<>();
        List<String> pendingDataList = new ArrayList<>();
        List<String> pendingForNextDayList = new ArrayList<>();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        String current_date = formatter.format(date).toString();
        LinkedHashMap<String,String> combinedMap1 = new LinkedHashMap<String,String>();
        LinkedHashMap<String,String> combinedMap2 = new LinkedHashMap<String,String>();
        Map<String, Iterable<String>> pendingDataMap = context.sideInput(pendingDataMapView);

        if (!pendingRecordCount.toString().trim().equals("0") || context.sideInput(recordCountView) != 0) {
            // Sort data
            LinkedHashMap<String, Iterable<String>> sortedData = context.sideInput(targetPCollectionView).entrySet()
                    .stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

            SortedMap<String, String> pendingSortedMapAll = new TreeMap<>();
            SortedMap<String, String> pendingSortedMap = new TreeMap<>();
            SortedMap<String, String> todaysSortedMapAll = new TreeMap<>();
            SortedMap<String, String> todaysSortedMap = new TreeMap<>();

            //pending data
            for (String key : pendingDataMap.keySet()) {

                // Put all pending data into sortedMap key = merchant_number+date
                for (String row : pendingDataMap.get(key)) {
                    if (row.substring(10, 13).equals("DTL")) {
                        pendingSortedMapAll.put(key.concat(row.substring(13, 29)).concat(row.substring(row.length()-8)).concat("1"), row);

                    } else {
                        pendingSortedMapAll.put(key.concat(row.substring(13, 29)).concat(row.substring(row.length()-8)).concat("2"), row);

                    }
                }

                //adding status of length one to key,  key = merchant_number+date+("1"/"2")+status
                for (String s : pendingSortedMapAll.keySet()) {
                    if (pendingSortedMapAll.get(s).toString().substring(10, 13).equals("DTL")) {
                        pendingSortedMap.put(s.concat(pendingSortedMapAll.get(s).substring(1037,1038)),pendingSortedMapAll.get(s));

                    } else {
                        pendingSortedMap.put(s.concat(pendingSortedMapAll.get(s.substring(0,s.length()-1).concat("1")).substring(1037,1038)),pendingSortedMapAll.get(s));

                    }
                }


            }

            //today's data
            for (String key : sortedData.keySet()) {

                // Put all todays data into sortedMap
                for (String row : sortedData.get(key)) {
                    if (row.toString().substring(10, 13).equals("DTL")) {
                        todaysSortedMapAll.put(key.concat(row.toString().substring(13, 29)).concat(current_date).concat("1"), row.toString().concat(current_date));

                    } else {
                        todaysSortedMapAll.put(key.concat(row.toString().substring(13, 29)).concat(current_date).concat("2"), row.toString().concat(current_date));

                    }
                }

                for (String s : todaysSortedMapAll.keySet()) {
                    if (todaysSortedMapAll.get(s).toString().substring(10, 13).equals("DTL")) {
                        todaysSortedMap.put(s.concat(todaysSortedMapAll.get(s).substring(1037,1038)),todaysSortedMapAll.get(s));

                    } else {
                        todaysSortedMap.put(s.concat(todaysSortedMapAll.get(s.substring(0,s.length()-1).concat("1")).substring(1037,1038)),todaysSortedMapAll.get(s));

                    }
                }

            }

            //putting all pending and today's data into combinedMap1 and removing date from key
            for (String pendingkey : pendingSortedMap.keySet()) {

                combinedMap1.put(pendingkey.substring(0,pendingkey.length()-10).concat(pendingkey.substring(pendingkey.length()-2)), pendingSortedMap.get(pendingkey));
            }

            for (String todayskey : todaysSortedMap.keySet()) {
                combinedMap1.put(todayskey.substring(0,todayskey.length()-10).concat(todayskey.substring(todayskey.length()-2)), todaysSortedMap.get(todayskey));

            }

            //separating pending and today's data
            for (String combinedMap1Key : combinedMap1.keySet()) {
                if(combinedMap2.containsKey(combinedMap1Key.substring(0,combinedMap1Key.length()-1))) {
                    StringBuilder pendingDataRow = new StringBuilder();
                    pendingDataRow.append(combinedMap1Key.substring(0,10)).append(",").append(combinedMap1.get(combinedMap1Key));
                    pendingForNextDayList.add(pendingDataRow.toString());
                }
                else
                    combinedMap2.put(combinedMap1Key.substring(0,combinedMap1Key.length()-1), combinedMap1.get(combinedMap1Key));
            }

            for (String finalkey : combinedMap2.keySet()) {
                if (combinedMap2.get(finalkey).substring(combinedMap2.get(finalkey).length()-8).equals(current_date)) {
                    StringBuilder dataRow = new StringBuilder();
                    dataRow.append(finalkey.substring(0,10)).append(",").append(combinedMap2.get(finalkey));
                    todaysDataList.add(dataRow.toString());
                }else {
                    StringBuilder dataRow = new StringBuilder();
                    dataRow.append(finalkey.substring(0,10)).append(",").append(combinedMap2.get(finalkey));
                    pendingDataList.add(dataRow.toString());
                }
            }

        }

        context.output(todaysDataTuple, String.join("\n", todaysDataList));
        if(pendingDataList.isEmpty())
            context.output(pendingDataTuple, "");
        else
            context.output(pendingDataTuple, String.join("\n", pendingDataList));

        if(pendingForNextDayList.isEmpty())
            context.output(pendingForNextDayTuple, "");
        else
            context.output(pendingForNextDayTuple, String.join("\n", pendingForNextDayList));
    }
}

