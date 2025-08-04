package com.database.replication.dto.response;

import java.util.List;
import java.util.stream.Collectors;

public record ResponseManyKeysValuesDto(List<KeyValue> keyValues) {

    public String serialize() {
        return keyValues.stream()
                .map(kv -> kv.key + ":" + kv.value)
                .collect(Collectors.joining(","));
    }

    public record KeyValue(Long key, String value) {}
}
