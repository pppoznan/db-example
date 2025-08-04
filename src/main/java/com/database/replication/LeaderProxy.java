package com.database.replication;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.domain.KeyValueEntry;
import com.database.domain.LSMTree;
import com.database.domain.WriteStorageProxy;

public class LeaderProxy implements WriteStorageProxy {
    private static final Logger LOGGER = Logger.getLogger(LeaderProxy.class.getName());

    private final LSMTree lsmTree;

    public LeaderProxy(LSMTree lsmTree) {
        this.lsmTree = lsmTree;
    }

    @Override
    public void put(KeyValueEntry keyValue) {
        LOGGER.log(Level.INFO, "LeaderProxy: PUT " + keyValue);
        lsmTree.put(keyValue);
    }

    @Override
    public void batchPut(List<KeyValueEntry> keyValues) {
        LOGGER.log(Level.INFO, "LeaderProxy: BATCH_PUT");
        lsmTree.batchPut(keyValues);
    }

    @Override
    public void delete(Long key) {
        LOGGER.log(Level.INFO, "LeaderProxy: DEL " + key);
        lsmTree.delete(key);
    }
}
