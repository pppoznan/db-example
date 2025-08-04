package com.database;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.config.NodeConfig;
import com.database.config.NodeConfigParser;
import com.database.replication.LeaderNode;
import com.database.replication.NodeAddress;
import com.database.replication.NodeRole;
import com.database.replication.ReplicaNode;

public class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());

    public static void main(String[] args) {
        LOGGER.log(Level.INFO, "Starting Application");

        NodeConfig nodeConfig = NodeConfigParser.parse(args);
        NodeRole role = nodeConfig.getRole();
        switch (role) {
            case LEADER -> {
                LOGGER.log(Level.INFO, "Starting Leader Node");
                startLeaderNode(nodeConfig);
            }
            case FOLLOWER -> {
                LOGGER.log(Level.INFO, "Starting Replica Node");
                startReplicaNode(nodeConfig);
            }
        }
    }

    private static void startReplicaNode(NodeConfig nodeConfig) {
        ReplicaNode leaderNode = new ReplicaNode(nodeConfig.getNodeId(), nodeConfig.getClientAddress(), nodeConfig.getSyncAddress());
        leaderNode.setReplicaNodes(nodeConfig.getPeerNodes());
        leaderNode.start();
    }

    private static void startLeaderNode(NodeConfig nodeConfig) {
        NodeAddress leaderClientAddress = nodeConfig.getClientAddress();
        LeaderNode leaderNode = new LeaderNode(nodeConfig.getNodeId(), leaderClientAddress);
        leaderNode.setReplicaNodes(nodeConfig.getPeerNodes());
        leaderNode.start();
    }
}
