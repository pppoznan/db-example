package com.database.replication.dto.command;

public record PutSingleKeyValueCommand(Long key, String value) {

    public static PutSingleKeyValueCommand deserialize(String serialized) {
        String[] splitted = serialized.split(":");
        return new PutSingleKeyValueCommand(Long.parseLong(splitted[0]), splitted[1]);
    }

}