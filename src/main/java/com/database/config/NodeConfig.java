package com.database.config;

import java.util.List;
import java.util.Objects;

import com.database.replication.NodeAddress;
import com.database.replication.NodeAddressWithId;
import com.database.replication.NodeRole;

/**
 * Represents the configuration for a single node in the distributed Key-Value system,
 * including its own details and the addresses of all peer nodes.
 */
public class NodeConfig {
    private final String nodeId;
    private final NodeAddress syncAddress;
    private final NodeAddress clientAddress;
    private final NodeRole role;
    private final List<NodeAddressWithId> peerNodes;

    public NodeConfig(String nodeId, NodeAddress clientAddress, NodeAddress syncAddress,
            NodeRole role, List<NodeAddressWithId> peerNodes
    ) {
        this.nodeId = Objects.requireNonNull(nodeId, "Node ID cannot be null");
        this.syncAddress = syncAddress;
        this.clientAddress = clientAddress;
        this.role = Objects.requireNonNull(role, "Node role cannot be null");
        this.peerNodes = Objects.requireNonNull(peerNodes, "Peer nodes list cannot be null");
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeAddress getClientAddress() {
        return clientAddress;
    }

    public NodeAddress getSyncAddress() {
        return syncAddress;
    }

    public NodeRole getRole() {
        return role;
    }

    public List<NodeAddressWithId> getPeerNodes() {
        return peerNodes;
    }
}
