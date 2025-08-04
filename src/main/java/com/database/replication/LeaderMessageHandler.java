package com.database.replication;

import java.util.List;

import com.database.domain.KeyValueEntry;
import com.database.domain.WriteStorageProxy;
import com.database.replication.dto.OperationDto;
import com.database.replication.dto.OperationTypeDeserializer;
import com.database.replication.dto.command.DelSingleKeyCommand;
import com.database.replication.dto.command.PutManyKeysValuesCommand;
import com.database.replication.dto.command.PutSingleKeyValueCommand;

public class LeaderMessageHandler implements CommandHandler {
    private final WriteStorageProxy writeStorageProxy;

    public LeaderMessageHandler(WriteStorageProxy writeStorageProxy) {
        this.writeStorageProxy = writeStorageProxy;
    }

    @Override
    public String handleCommand(String operationRaw) {
        OperationDto operation = OperationDto.deserialize(operationRaw);
        return switch (operation.type()) {
            case HEARTBEAT -> "OK";
            case PUT -> {
                PutSingleKeyValueCommand deserializedData = OperationTypeDeserializer.PUT.deserialize(operation.data());
                writeStorageProxy.put(
                        new KeyValueEntry(
                                deserializedData.key(),
                                deserializedData.value()
                        )
                );
                yield "OK";
            }
            case BATCH_PUT -> {
                PutManyKeysValuesCommand deserializedData = OperationTypeDeserializer.BATCH_PUT.deserialize(operation.data());
                List<KeyValueEntry> keyValueEntries = deserializedData.keyValues().stream()
                        .map(kv -> new KeyValueEntry(kv.key(), kv.value()))
                        .toList();
                writeStorageProxy.batchPut(keyValueEntries);
                yield "OK";
            }
            case DEL -> {
                DelSingleKeyCommand deserializedData = OperationTypeDeserializer.DEL.deserialize(operation.data());
                writeStorageProxy.delete(deserializedData.key());
                yield "OK";
            }
            default -> "UNEXPECTED_COMMAND";
        };
    }
}
