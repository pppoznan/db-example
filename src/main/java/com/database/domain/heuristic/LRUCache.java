package com.database.domain.heuristic;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LRUCache<K, V> {
    private final int capacity;
    private final Map<K, Node<K, V>> cacheMap;
    private Node<K, V> head;
    private Node<K, V> tail;

    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Cache capacity must be positive.");
        }
        this.capacity = capacity;
        this.cacheMap = new HashMap<>();
        this.head = new Node<>(null, null);
        this.tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    public Optional<V> get(K key) {
        Node<K, V> node = cacheMap.get(key);
        if (node == null) {
            return Optional.empty();
        }
        moveToFront(node);
        return Optional.ofNullable(node.value);
    }

    public void put(K key, V value) {
        Node<K, V> existingNode = cacheMap.get(key);

        if (existingNode != null) {
            existingNode.value = value;
            moveToFront(existingNode);
        } else {
            // Key is new
            Node<K, V> newNode = new Node<>(key, value);
            cacheMap.put(key, newNode);
            addNodeToFront(newNode);

            if (cacheMap.size() > capacity) {
                removeLRU();
            }
        }
    }

    private void moveToFront(Node<K, V> node) {
        removeNode(node);
        addNodeToFront(node);
    }

    private void removeNode(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void addNodeToFront(Node<K, V> node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private K removeLRU() {
        if (head.next == tail) {
            return null;
        }
        Node<K, V> lruNode = tail.prev;
        removeNode(lruNode);
        cacheMap.remove(lruNode.key);
        return lruNode.key;
    }

    private static class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
            this.prev = null;
            this.next = null;
        }
    }
}