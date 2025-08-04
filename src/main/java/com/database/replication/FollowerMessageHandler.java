package com.database.replication;

import com.database.domain.ReadStorageProxy;
import com.database.replication.dto.OperationDto;
import com.database.replication.dto.OperationTypeDeserializer;
import com.database.replication.dto.command.GetKeyRangeCommand;
import com.database.replication.dto.command.GetSingleKeyCommand;
import com.database.replication.dto.response.ResponseManyKeysValuesDto;
import com.database.replication.dto.response.ResponseSingleKeyValueDto;

public class FollowerMessageHandler implements CommandHandler {
    private final ReadStorageProxy readStorageProxy;

    public FollowerMessageHandler(ReadStorageProxy readStorageProxy) {
        this.readStorageProxy = readStorageProxy;
    }

    @Override
    public String handleCommand(String operationRaw) {
        OperationDto operation = OperationDto.deserialize(operationRaw);
        return switch (operation.type()) {
            case GET -> {
                GetSingleKeyCommand deserializedData = OperationTypeDeserializer.GET.deserialize(operation.data());
                ResponseSingleKeyValueDto responseData = readStorageProxy.read(deserializedData.key());
                yield responseData.serialize();
            }
            case GET_RANGE -> {
                GetKeyRangeCommand deserializedData = OperationTypeDeserializer.GET_RANGE.deserialize(operation.data());
                ResponseManyKeysValuesDto responseData = readStorageProxy.readRange(deserializedData.from(), deserializedData.to());
                yield responseData.serialize();
            }
            default -> "UNEXPECTED_COMMAND";
        };
    }

}
