package com.database.domain;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Parameters {
    public static String maxLogOperationLength(String string) {
        return string != null && string.length() > 20
                ? string.substring(0, 20) + "..."
                : string;
    }

    public static final int NUM_OF_THREADS_FOR_REPLICA_MESSAGES = 20;
    public static final int NUM_OF_THREADS_FOR_LEADER_MESSAGES = 10;
    public static final int NUM_OF_THREADS_FOR_RECEIVING_MESSAGES = 5;

    public static final int DEFAULT_MEM_TABLE_MAX_SIZE = 10_000;
    public static final int LRU_CACHE_SIZE = 10_000;

    public static final int MAX_MISSING_HEARTBEATS_TO_START_ELECTION = 3;
    public static final int MAX_MISSING_ATTEMPTS_TO_START_RE_ELECTION = 3 * MAX_MISSING_HEARTBEATS_TO_START_ELECTION;


    public static ScheduledExecutorService executorServiceForSendingHeartbeats(int replicaNodes) {
        return Executors.newScheduledThreadPool(replicaNodes);
    }

    public static ExecutorService executorServiceForReceivingMessages() {
        return Executors.newFixedThreadPool(NUM_OF_THREADS_FOR_RECEIVING_MESSAGES);
    }
}
