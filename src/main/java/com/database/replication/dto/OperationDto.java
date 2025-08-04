package com.database.replication.dto;

public record OperationDto(OperationTypeDto type, String data) {

    public static OperationDto deserialize(String serialized) {
        String[] splitted = serialized.split("\\|", 2);

        OperationTypeDto operationType = OperationTypeDto.valueOf(splitted[0]);
        return new OperationDto(
                operationType,
                splitted[1]
        );
    }

    public String serialize() {
        return type.name() + "|" + data;
    }
}
