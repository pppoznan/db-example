package com.database.domain;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PersistMemTable implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(PersistMemTable.class.getName());

    private final MemTable<KeyValueEntry> memTable;
    private final PersistentStorage persistentStorage;

    public PersistMemTable(MemTable<KeyValueEntry> memTable,
            PersistentStorage persistentStorage
    ) {
        this.persistentStorage = persistentStorage;
        this.memTable = memTable;
    }

    @Override
    public void run() {
        LOGGER.log(Level.INFO, "Persisting MemTable");
        persistMemTables();
    }

    private void persistMemTables() {
        persistentStorage.update(memTable.getEntries());
    }
}
