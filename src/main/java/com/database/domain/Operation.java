package com.database.domain;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public record Operation(PersistableOperationType type, List<KeyValueEntry> entries) {

    public Operation(PersistableOperationType type, KeyValueEntry entry) {
        this(type, List.of(entry));
    }

    public static Operation deserialize(String serialized) {
        String[] splitted = serialized.split("\\|");
        List<KeyValueEntry> entries = Arrays.stream(splitted[1]
                        .split(","))
                .map(entry -> entry.split(":"))
                .map(entry -> {
                    if (entry.length == 1) {
                        return new KeyValueEntry(Long.parseLong(entry[0]), "");
                    }
                    return new KeyValueEntry(Long.parseLong(entry[0]), entry[1]);
                })
                .toList();
        return new Operation(
                PersistableOperationType.valueOf(splitted[0]),
                entries
        );
    }

    public String serialize() {
        return type.name() + "|" +
                entries.stream()
                        .map(KeyValueEntry::serialize)
                        .collect(Collectors.joining(","));
    }
}
