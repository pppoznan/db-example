package com.database.replication;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.domain.Parameters;

public class BroadcasterClientCommands {
    private static final Logger LOGGER = Logger.getLogger(BroadcasterClientCommands.class.getName());

    private final Collection<NodeAddressWithId> replicaNodes;

    public BroadcasterClientCommands(Collection<NodeAddressWithId> replicaNodes) {
        this.replicaNodes = replicaNodes;
    }

    void broadcast(String operationRaw) {
        for (NodeAddressWithId replicaNode : replicaNodes) {
            String replicaHost = replicaNode.address().host();
            int replicaPort = replicaNode.address().port();
            try (Socket socket = new Socket(replicaHost, replicaPort);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                LOGGER.log(Level.INFO, "Broadcasting operation: " + Parameters.maxLogOperationLength(operationRaw) + " to " + replicaHost + ":" + replicaPort);
                out.write(operationRaw);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error broadcasting operation: " + operationRaw + " to " + replicaHost + ":" + replicaPort, e);
            }
        }
    }
}
