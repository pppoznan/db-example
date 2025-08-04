package com.database.replication.dto.command;

public record DelSingleKeyCommand(Long key) {

    public static DelSingleKeyCommand deserialize(String serialized) {
        return new DelSingleKeyCommand(Long.parseLong(serialized));
    }
}