package com.database.replication.dto.command;

public record GetSingleKeyCommand(Long key) {

    public static GetSingleKeyCommand deserialize(String serialized) {
        return new GetSingleKeyCommand(Long.parseLong(serialized));
    }
}