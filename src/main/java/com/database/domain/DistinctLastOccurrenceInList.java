package com.database.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DistinctLastOccurrenceInList {

    public static List<KeyValueEntry> prune(List<KeyValueEntry> originalList) {
        Map<Long, KeyValueEntry> distinctElementsMap = new LinkedHashMap<>();

        for (int i = originalList.size() - 1; i >= 0; i--) {
            KeyValueEntry element = originalList.get(i);
            distinctElementsMap.putIfAbsent(element.key(), element);
        }
        List<KeyValueEntry> result = new ArrayList<>(distinctElementsMap.values());
        Collections.reverse(result);
        return result;
    }
}