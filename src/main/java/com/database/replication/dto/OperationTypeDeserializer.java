package com.database.replication.dto;

import java.util.function.Function;

import com.database.replication.dto.command.DelSingleKeyCommand;
import com.database.replication.dto.command.GetKeyRangeCommand;
import com.database.replication.dto.command.GetSingleKeyCommand;
import com.database.replication.dto.command.PutManyKeysValuesCommand;
import com.database.replication.dto.command.PutSingleKeyValueCommand;

public class OperationTypeDeserializer<T> {

    public static final OperationTypeDeserializer<PutSingleKeyValueCommand> PUT = new OperationTypeDeserializer<>(
            PutSingleKeyValueCommand::deserialize
    );
    public static final OperationTypeDeserializer<PutManyKeysValuesCommand> BATCH_PUT = new OperationTypeDeserializer<>(
            PutManyKeysValuesCommand::deserialize
    );
    public static final OperationTypeDeserializer<DelSingleKeyCommand> DEL = new OperationTypeDeserializer<>(
            DelSingleKeyCommand::deserialize
    );
    public static final OperationTypeDeserializer<GetSingleKeyCommand> GET = new OperationTypeDeserializer<>(
            GetSingleKeyCommand::deserialize
    );
    public static final OperationTypeDeserializer<GetKeyRangeCommand> GET_RANGE = new OperationTypeDeserializer<>(
            GetKeyRangeCommand::deserialize
    );

    private final Function<String, T> deserializer;

    OperationTypeDeserializer(Function<String, T> deserializer) {
        this.deserializer = deserializer;
    }

    public T deserialize(String serialized) {
        return deserializer.apply(serialized);
    }
}
