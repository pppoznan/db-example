package com.database.replication.dto.response;

public record ResponseSingleKeyValueDto(Long key, String value) {

    public String serialize() {
        return key + ":" + value;
    }
}
