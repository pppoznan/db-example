package com.database.config;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import com.database.replication.NodeAddress;
import com.database.replication.NodeAddressWithId;
import com.database.replication.NodeRole;

public class NodeConfigParser {
    private static final Logger LOGGER = Logger.getLogger(NodeConfigParser.class.getName());

    public static NodeConfig parse(String[] args) {
        if (args.length == 0) {
            System.err.println("No arguments provided.");
        }

        Map<String, String> parsedArgs = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String key = arg.substring(2); // Remove "--"
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    parsedArgs.put(key, args[i + 1]);
                    i++; // Consume the value
                } else {
                    System.err.println("Missing value for argument: " + arg);
                }
            } else {
                System.err.println("Unexpected argument: " + arg + ". Arguments must start with '--'.");
            }
        }

        String roleStr = getRequiredArg(parsedArgs, "role");
        String portStr = getRequiredArg(parsedArgs, "port");
        String syncPortStr = roleStr.equals("LEADER")
                ? "0"
                : getRequiredArg(parsedArgs, "sync-port");
        String nodeId = getRequiredArg(parsedArgs, "id");
        String peersStr = parsedArgs.get("peers"); // Peers is optional

        NodeRole role;
        int port;
        int syncPort;

        try {
            role = NodeRole.valueOf(roleStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid role: '" + roleStr + "'. Must be LEADER or FOLLOWER.", e);
        }

        try {
            port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: '" + portStr + "'. Must be an integer.", e);
        }

        try {
            syncPort = Integer.parseInt(syncPortStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: '" + syncPortStr + "'. Must be an integer.", e);
        }

        List<NodeAddressWithId> peerNodes = new ArrayList<>();
        if (peersStr != null && !peersStr.isEmpty()) {
            String[] peerStrings = peersStr.split(",");
            for (String peerString : peerStrings) {
                String[] parts = peerString.split(":");
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Malformed peer address: '" + peerString + "'. Expected format: id:host:port.");
                }
                try {
                    String peerId = parts[0];
                    String peerHost = parts[1];
                    int peerPort = Integer.parseInt(parts[2]);
                    peerNodes.add(new NodeAddressWithId(peerId, new NodeAddress(peerHost, peerPort)));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid peer port number in '" + peerString + "'. Must be an integer.", e);
                }
            }
        }

        LOGGER.info("Parsed arguments for node: ID=" + nodeId + ", Role=" + role + ", Port=" + port + ", SyncPort=" + syncPort);
        LOGGER.info("Parsed peer nodes: " + peerNodes);

        return new NodeConfig(nodeId, new NodeAddress("127.0.0.1", port), new NodeAddress("127.0.0.1", syncPort), role, peerNodes);
    }

    private static String getRequiredArg(Map<String, String> args, String key) {
        String value = args.get(key);
        if (value == null || value.isEmpty()) {
            System.err.println("Missing required argument: --" + key);
        }
        return value;
    }
}
