package com.database.domain;

import static com.database.domain.Parameters.DEFAULT_MEM_TABLE_MAX_SIZE;
import static com.database.domain.Parameters.LRU_CACHE_SIZE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.database.domain.heuristic.LRUCache;

public class LSMTree {
    private final PersistentStorage persistentStorage;
    private final LRUCache<Long, KeyValueEntry> LRUCache;
    private final WriteAheadLog writeAheadLogFile;
    private final LogicalTimeProvider logicalTimeProvider;
    private final ExecutorService executorService;
    private final WriteLock writeLock;

    private MemTable<KeyValueEntry> memTable;

    public LSMTree(String storageDirectory) {
        this.logicalTimeProvider = new LogicalTimeProvider();
        this.persistentStorage = new PersistentStorage(storageDirectory);
        this.executorService = Executors.newFixedThreadPool(1);
        this.memTable = newMemTable();
        this.LRUCache = new LRUCache<>(LRU_CACHE_SIZE);

        this.writeAheadLogFile = new WriteAheadLog(storageDirectory, logicalTimeProvider);
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.writeLock = reentrantReadWriteLock.writeLock();

        replayFromWriteAheadLogFiles();
    }

    private void replayFromWriteAheadLogFiles() {
        writeAheadLogFile.newLogFile();

        List<Operation> uncommitedOperations = writeAheadLogFile.getUncommitedOperations();
        for (Operation operation : uncommitedOperations) {
            switch (operation.type()) {
                case PUT -> put(operation.entries().getFirst());
                case BATCH_PUT -> batchPut(operation.entries());
                case DEL -> delete(operation.entries().getFirst().key());
            }
        }
        writeAheadLogFile.removePreviousLogFiles(logicalTimeProvider.getVersion());
    }

    public void batchPut(List<KeyValueEntry> keyValues) {
        try {
            this.writeLock.lock();
            List<KeyValueEntry> prunedKeyValues = prunKeyValues(keyValues);
            prunedKeyValues.forEach(this::put);
        } finally {
            this.writeLock.unlock();
        }
    }

    public static List<KeyValueEntry> prunKeyValues(List<KeyValueEntry> keyValues) {
        return DistinctLastOccurrenceInList.prune(keyValues);
    }

    public void put(KeyValueEntry keyValue) {
        writeAheadLogFile.append(new Operation(PersistableOperationType.PUT, keyValue).serialize());

        memTable.add(keyValue.key(), keyValue);
        LRUCache.put(keyValue.key(), keyValue);

        persistMemTableIfFull();
    }

    public KeyValueEntry read(Long key) {
        //TODO: using library, we could mark method as Nullable and return null,
        // because Optional adds overhead in terms of memory and GC process
        Optional<KeyValueEntry> valueFromMemTable = memTable.get(key);
        if (valueFromMemTable.isPresent()) {
            return valueFromMemTable.get();
        }

        Optional<KeyValueEntry> valueFromLRU = LRUCache.get(key);
        if (valueFromLRU.isPresent()) {
            return valueFromLRU.get();
        }

        Optional<KeyValueEntry> valueFromPersistentStorage = persistentStorage.read(key);

        KeyValueEntry returnedValue = valueFromPersistentStorage
                .orElse(KeyValueEntry.empty(key));

        LRUCache.put(key, returnedValue);

        return returnedValue;
    }

    public List<KeyValueEntry> readRange(Long from, Long to) {
        if (from > to) {
            throw new IllegalArgumentException("'from' cannot be greater than 'to'");
        }
        Map<Long, KeyValueEntry> entriesInMemTable = memTable.rangeScan(from, to);

        List<Long> keysToFetch = new ArrayList<>();
        LongStream.range(from, to + 1)
                .forEach(value -> {
                    if (!entriesInMemTable.containsKey(value)) {
                        keysToFetch.add(value);
                    }
                });
        List<KeyValueEntry> entriesInPersistentStorage = persistentStorage.readMany(keysToFetch);

        return Stream.of(entriesInMemTable.values(), entriesInPersistentStorage)
                .flatMap(Collection::stream)
                .sorted(Comparator.comparing(KeyValueEntry::key))
                .filter(e -> !Objects.equals(e.value(), KeyValueEntry.EMPTY_VALUE))
                .collect(Collectors.toList());
    }

    public void delete(Long key) {
        KeyValueEntry entry = new KeyValueEntry(key);
        try {
            this.writeLock.lock();
            writeAheadLogFile.append(new Operation(PersistableOperationType.DEL, entry).serialize());
            memTable.add(key, entry);
            LRUCache.put(key, KeyValueEntry.empty(key));
        } finally {
            this.writeLock.unlock();
        }
    }

    private void persistMemTableIfFull() {
        if (memTable.isFull()) {
            try {
                this.writeLock.lock();
                if (memTable.isFull()) {
                    MemTable<KeyValueEntry> memTableToPersist = memTable;
                    memTable = newMemTable();

                    logicalTimeProvider.incrementVersion();
                    writeAheadLogFile.newLogFile();

                    executorService.submit(() -> {
                        new PersistMemTable(memTableToPersist, persistentStorage).run();
                        writeAheadLogFile.removePreviousLogFiles(logicalTimeProvider.getVersion());
                    });
                }
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    private static MemTable<KeyValueEntry> newMemTable() {
        return new MemTable<>(DEFAULT_MEM_TABLE_MAX_SIZE);
    }
}
