package com.database.replication;

import static com.database.domain.Parameters.NUM_OF_THREADS_FOR_LEADER_MESSAGES;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.database.domain.LSMTree;
import com.database.domain.Parameters;
import com.database.replication.dto.OperationDto;
import com.database.replication.dto.OperationTypeDto;

public final class LeaderNode {
    private final LSMTree lsmTree;

    private final ClientHandler node;
    private BroadcasterClientCommands broadcaster;
    private Collection<NodeAddressWithId> replicaNodes;

    public LeaderNode(String id, NodeAddress clientAddress) {
        ExecutorService leaderExecutor = Executors.newFixedThreadPool(NUM_OF_THREADS_FOR_LEADER_MESSAGES);
        this.node = new ClientHandler("node-" + id, clientAddress, NodeRole.LEADER, leaderExecutor);
        this.lsmTree = new LSMTree("data_node_" + id);
    }

    public void start() {
        ExecutorService executorService = Parameters.executorServiceForReceivingMessages();
        executorService.submit(this::run);

        ScheduledExecutorService scheduledExecutorService = Parameters.executorServiceForSendingHeartbeats(replicaNodes.size());
        scheduledExecutorService
            .scheduleAtFixedRate(this::sendHeartbeat, 30, 10, TimeUnit.SECONDS);
    }

    public void setReplicaNodes(Collection<NodeAddressWithId> replicaNodes) {
        this.replicaNodes = replicaNodes;
        this.broadcaster = new BroadcasterClientCommands(replicaNodes);
    }

    private void run() {
        LeaderMessageHandler leaderMessageHandler = new LeaderMessageHandler(new LeaderProxy(lsmTree));
        node.consumeCommand(leaderMessageHandler, operationRaw -> broadcaster.broadcast(operationRaw));
    }

    private void sendHeartbeat() {
        broadcaster.broadcast(new OperationDto(OperationTypeDto.HEARTBEAT, "empty").serialize());
    }
}
