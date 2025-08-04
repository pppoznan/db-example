package com.database;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.database.replication.NodeAddress;

public class Client {
    private static final Logger LOGGER = Logger.getLogger(Client.class.getName());
    private final NodeAddress leaderAddress;
    private final Set<NodeAddress> replicaNodes;

    public Client(NodeAddress leaderAddress, Set<NodeAddress> replicaNodes) {
        this.leaderAddress = leaderAddress;
        this.replicaNodes = replicaNodes;

        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.INFO);
        for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                handler.setLevel(Level.INFO);
            }
        }
    }

    public void put(Long key, String value) {
        try (Socket socket = new Socket(leaderAddress.host(), leaderAddress.port());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            System.out.printf("Leader Node: PUT|%d:%s%n", key, value);
            out.println("PUT|" + key +":" + value);
            String response = in.readLine();
            System.out.printf("Leader Node: response=%s%n", response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Client PUT error: " + e.getMessage(), e);
        }
    }

    public void batchPut(Collection<KeyValue> keyValues) {
        try (Socket socket = new Socket(leaderAddress.host(), leaderAddress.port());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            String operation = keyValues.stream()
                    .map(kv -> kv.key() + ":" + kv.value())
                    .collect(Collectors.joining(","));
            System.out.println("Leader Node: BATCH_PUT|" + operation);
            out.println("BATCH_PUT|" + operation);
            String response = in.readLine();
            System.out.printf("Leader Node: response=%s%n", response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Client PUT error: " + e.getMessage(), e);
        }
    }

    record KeyValue(Long key, String value) {}

    public void get(Long key) {
        for (NodeAddress replicaNode : replicaNodes) {
            try (Socket socket = new Socket(replicaNode.host(), replicaNode.port());
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                System.out.printf("Replica Node: %s:%d GET|key=%d%n", replicaNode.host(), replicaNode.port(), key);
                out.println("GET|" + key);
                String response = in.readLine();
                System.out.printf("Replica Node: %s:%d response=%s%n", replicaNode.host(), replicaNode.port(), response);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Client GET error: " + e.getMessage(), e);
            }
        }
    }

    public void delete(Long key) {
        try (Socket socket = new Socket(leaderAddress.host(), leaderAddress.port());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            System.out.println("Leader Node: DEL|" + key);
            out.println("DEL|" + key);
            String response = in.readLine();
            System.out.printf("Leader Node: response=%s%n", response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Client DEL error: " + e.getMessage(), e);
        }
    }

    private void getRange(long from, long to) {
        for (NodeAddress replicaNode : replicaNodes) {
            try (Socket socket = new Socket(replicaNode.host(), replicaNode.port());
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                System.out.printf("Replica Node: %s:%d GET_RANGE|%d:%d%n", replicaNode.host(), replicaNode.port(), from, to);
                out.println("GET_RANGE|" + from + ":" + to);
                String response = in.readLine();
                System.out.printf("Replica Node: %s:%d response=%s%n", replicaNode.host(), replicaNode.port(), response);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Client GET_RANGE error: " + e.getMessage(), e);
            }
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Client client = new Client(new NodeAddress("127.0.0.1", 27000),
                Set.of(
                        new NodeAddress("127.0.0.1", 27001),
                        new NodeAddress("127.0.0.1", 27002)
                )
        );

        System.out.println("--- Client Operations ---");

        List<KeyValue> keyValues = new LinkedList<>();
        for (long i = 100_000; i < 1_000_000; i += 10) {
            keyValues.add(new KeyValue(i, "__value__" + i));
        }

        long start = System.currentTimeMillis();
        client.batchPut(keyValues);
        long end = System.currentTimeMillis();
        System.out.println("Time taken BATCH_PUT: " + (end - start) + "ms");

        //wait for replication a bit
        Thread.sleep(2_500);

        start = System.currentTimeMillis();
        client.getRange(950_000L, 955_000L);
        end = System.currentTimeMillis();
        System.out.println("Time taken GET_RANGE: " + (end - start) + "ms");

        client.get(100_000L);

        client.delete(100_000L);

        client.get(100_000L);
    }
}
