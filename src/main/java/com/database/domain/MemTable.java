package com.database.domain;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MemTable<V> {
    private TreeMap<Long, V> entries;
    private int maxSize;
    private AtomicInteger currentSize;

    public MemTable(int maxSize) {
        this.entries = new TreeMap<>();
        this.maxSize = maxSize;
        this.currentSize = new AtomicInteger(0);
    }

    public void add(Long key, V value) {
        if (!entries.containsKey(key)) {
            currentSize.incrementAndGet();
        }
        entries.put(key, value);
    }

    public Optional<V> get(Long key) {
        return Optional.ofNullable(entries.get(key));
    }

    public boolean isFull() {
        return currentSize.get() >= maxSize;
    }

    public Set<Entry<Long, V>> getEntries() {
        return entries.entrySet();
    }

    public Map<Long, V> rangeScan(Long startKey, Long endKey) {
        return entries.subMap(startKey, true, endKey, true);
    }

    public int size() {
        return currentSize.get();
    }
}