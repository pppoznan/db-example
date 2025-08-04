package com.database.replication;

public interface CommandHandler {
    String handleCommand(String operationRaw);
}
