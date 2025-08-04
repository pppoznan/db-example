package com.database.replication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.domain.Parameters;

public final class ClientHandler {
    private static final Logger LOGGER = Logger.getLogger(ClientHandler.class.getName());

    private final String id;
    private final NodeAddress clientAddress;
    private final NodeRole role;
    private final ExecutorService executorService;
    private ServerSocket serverSocket;

    public ClientHandler(String id, NodeAddress clientAddress, NodeRole role, ExecutorService executorService) {
        this.id = id;
        this.clientAddress = clientAddress;
        this.role = role;
        this.executorService = executorService;
    }

    public void consumeCommand(CommandHandler commandHandler) {
        consumeCommand(commandHandler, operationRaw -> {});
    }

    public void consumeCommand(CommandHandler commandHandler, CommandPostHandler commandPostHandler) {
        try {
            this.serverSocket = new ServerSocket(clientAddress.port(), 50, InetAddress.getByName(clientAddress.host()));

            LOGGER.log(Level.INFO,"Node " + id + " starting on port " + clientAddress.port() + " as " + role);
            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleMessage(commandHandler, commandPostHandler, clientSocket));
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error: " + e.getMessage());
        } finally {
            stopConsuming();
        }
    }

    public void stopConsuming() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error: " + e.getMessage());
        }
    }

    private void handleMessage(CommandHandler commandHandler, CommandPostHandler commandPostHandler, Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))
        ) {
            String operationRaw = in.readLine();
            LOGGER.log(Level.INFO, "Node: " + id + " received command: " + Parameters.maxLogOperationLength(operationRaw) + " from " + clientSocket.getInetAddress());

            String response = commandHandler.handleCommand(operationRaw);
            commandPostHandler.accept(operationRaw);

            LOGGER.log(Level.INFO, "Node: " + id + " handled command: " + Parameters.maxLogOperationLength(operationRaw) + " from " + clientSocket.getInetAddress());

            if (response != null) {
                out.write(response);
                out.flush();
                LOGGER.log(Level.INFO,"Node: " + id + " returned response: " + Parameters.maxLogOperationLength(response) + " from " + clientSocket.getInetAddress());
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public interface CommandPostHandler extends Consumer<String> {
        void accept(String operationRaw);
    }
}
