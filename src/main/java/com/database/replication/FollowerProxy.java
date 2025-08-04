package com.database.replication;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.domain.KeyValueEntry;
import com.database.domain.LSMTree;
import com.database.domain.ReadStorageProxy;
import com.database.replication.dto.response.ResponseManyKeysValuesDto;
import com.database.replication.dto.response.ResponseSingleKeyValueDto;

public class FollowerProxy implements ReadStorageProxy {
    private static final Logger LOGGER = Logger.getLogger(FollowerProxy.class.getName());

    private final LSMTree lsmTree;

    public FollowerProxy(LSMTree lsmTree) {
        this.lsmTree = lsmTree;
    }

    @Override
    public ResponseSingleKeyValueDto read(Long key) {
        LOGGER.log(Level.INFO, "FollowerProxy: GET " + key);
        KeyValueEntry entry = lsmTree.read(key);
        return new ResponseSingleKeyValueDto(entry.key(), entry.value());
    }

    @Override
    public ResponseManyKeysValuesDto readRange(Long from, Long to) {
        LOGGER.log(Level.INFO, "FollowerProxy: GET_RANGE " + from + " : " + to);
        List<KeyValueEntry> entries = lsmTree.readRange(from, to);
        return new ResponseManyKeysValuesDto(
                entries.stream()
                        .map(kv -> new ResponseManyKeysValuesDto.KeyValue(kv.key(), kv.value()))
                        .toList()
        );
    }
}
