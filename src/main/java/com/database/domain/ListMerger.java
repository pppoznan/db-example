package com.database.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ListMerger {

    public static List<KeyValueEntry> mergeListsPrioritizingNewData(
            List<KeyValueEntry> existingData,
            List<KeyValueEntry> newData) {

        TreeMap<Long, String> mergedMap = new TreeMap<>();

        for (KeyValueEntry entry : existingData) {
            mergedMap.put(entry.key(), entry.value());
        }

        for (KeyValueEntry entry : newData) {
            mergedMap.put(entry.key(), entry.value());
        }

        List<KeyValueEntry> result = new ArrayList<>(mergedMap.size());
        for (Map.Entry<Long, String> entry : mergedMap.entrySet()) {
            result.add(new KeyValueEntry(entry.getKey(), entry.getValue()));
        }

        return result;
    }

}
