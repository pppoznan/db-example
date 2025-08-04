package com.database.domain;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class LogicalTimeProvider implements ReadOnlyLogicalTimeProvider {
    private final AtomicLong currentTime = new AtomicLong(Instant.now().toEpochMilli());

    public long getVersion() {
        return currentTime.get();
    }

    void incrementVersion() {
        currentTime.incrementAndGet();
    }
}
