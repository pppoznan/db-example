package com.database.replication.dto.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public record PutManyKeysValuesCommand(Collection<KeyValue> keyValues) {

    public static PutManyKeysValuesCommand deserialize(String serialized) {
        String[] splittedKeys = serialized.split(",");
        List<KeyValue> keyValues = new ArrayList<>();

        for (String keyValue : splittedKeys) {
            String[] splitted = keyValue.split(":");
            keyValues.add(new KeyValue(Long.parseLong(splitted[0]), splitted[1]));
        }
        return new PutManyKeysValuesCommand(keyValues);
    }

    public record KeyValue(Long key, String value) {}

}