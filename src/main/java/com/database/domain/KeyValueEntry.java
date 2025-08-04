package com.database.domain;

import java.util.Objects;

public class KeyValueEntry implements Comparable<KeyValueEntry> {
    public static final String EMPTY_VALUE = "";

    private final Long key;
    private final String value;

    public KeyValueEntry(Long key, String value) {
        this.key = key;
        this.value = value;
    }

    public KeyValueEntry(Long key) {
        this(key, EMPTY_VALUE);
    }

    public Long key() {
        return key;
    }

    public String value() {
        return value;
    }

    String serialize() {
        return key + ":" + value;
    }

    public static KeyValueEntry empty(Long key) {
        return new KeyValueEntry(key, EMPTY_VALUE);
    }

    @Override
    public int compareTo(KeyValueEntry o) {
        return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {return false;}
        KeyValueEntry entry = (KeyValueEntry) o;
        return Objects.equals(key, entry.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public String toString() {
        return "KeyValueEntry{" +
                "key=" + key +
                ", value='" + value + '\'' +
                '}';
    }
}