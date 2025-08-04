package com.database.replication.dto.command;

public record GetKeyRangeCommand(Long from, Long to) {

    public static GetKeyRangeCommand deserialize(String serialized) {
        String[] splitted = serialized.split(":");
        return new GetKeyRangeCommand(Long.parseLong(splitted[0]), Long.parseLong(splitted[1]));
    }
}