package com.database.domain;

import java.util.List;

public interface WriteStorageProxy {
    void put(KeyValueEntry keyValue);
    void batchPut(List<KeyValueEntry> keyValues);
    void delete(Long key);
}
