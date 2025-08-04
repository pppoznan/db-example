package com.database.replication;

import static com.database.domain.Parameters.MAX_MISSING_ATTEMPTS_TO_START_RE_ELECTION;
import static com.database.domain.Parameters.MAX_MISSING_HEARTBEATS_TO_START_ELECTION;
import static com.database.domain.Parameters.NUM_OF_THREADS_FOR_LEADER_MESSAGES;
import static com.database.domain.Parameters.NUM_OF_THREADS_FOR_REPLICA_MESSAGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.domain.LSMTree;
import com.database.domain.Parameters;
import com.database.replication.ClientHandler.CommandPostHandler;
import com.database.replication.dto.OperationDto;
import com.database.replication.dto.OperationTypeDto;

public final class ReplicaNode {
    private static final Logger LOGGER = Logger.getLogger(ReplicaNode.class.getName());

    private final LSMTree lsmTree;

    private final NodeAddress syncAddress;
    private final NodeAddress clientAddress;
    private final ExecutorService replicaExecutor;
    private final ExecutorService leaderExecutor;
    private final AtomicInteger missingHeartbeatCounter = new AtomicInteger(0);
    private ClientHandler clientNode;
    private ClientHandler syncNode;
    private NodeRole nodeRole;
    private Collection<NodeAddressWithId> replicaNodes;

    private BroadcasterClientCommands broadcaster;

    private final Collection<ExecutorService> executorServices = new ArrayList<>();

    public ReplicaNode(String id, NodeAddress clientAddress, NodeAddress syncAddress) {
        this.nodeRole = NodeRole.FOLLOWER;
        this.syncAddress = syncAddress;
        this.clientAddress = clientAddress;
        this.replicaExecutor = Executors.newFixedThreadPool(NUM_OF_THREADS_FOR_REPLICA_MESSAGES);
        this.leaderExecutor = Executors.newFixedThreadPool(NUM_OF_THREADS_FOR_LEADER_MESSAGES);
        this.clientNode = new ClientHandler("node-" + id, clientAddress, nodeRole, replicaExecutor);
        this.syncNode = new ClientHandler("sync-node-" + id, syncAddress, nodeRole, leaderExecutor);
        this.lsmTree = new LSMTree("data_node_" + id);
    }

    public void start() {
        ExecutorService executorService = Parameters.executorServiceForReceivingMessages();
        switch (nodeRole) {
            case LEADER -> executorService.submit(this::handleClientCommandsAsLeader);
            case FOLLOWER -> {
                executorService.submit(this::handleClientCommandsAsReplica);
                executorService.submit(this::handleSyncCommands);
            }
        }

        ScheduledExecutorService scheduledExecutorService =
                Parameters.executorServiceForSendingHeartbeats(replicaNodes.size());
        switch (nodeRole) {
            case LEADER -> scheduledExecutorService
                    .scheduleAtFixedRate(this::sendHeartbeat, 30, 10, TimeUnit.SECONDS);
            case FOLLOWER -> scheduledExecutorService
                    .scheduleAtFixedRate(this::shouldStartElection, 40, 15, TimeUnit.SECONDS);
        }

        executorServices.add(executorService);
        executorServices.add(scheduledExecutorService);
    }

    public void setReplicaNodes(Collection<NodeAddressWithId> replicaNodes) {
        this.replicaNodes = replicaNodes;
    }

    private void handleClientCommandsAsReplica() {
        FollowerMessageHandler followerMessageHandler = new FollowerMessageHandler(new FollowerProxy(lsmTree));
        clientNode.consumeCommand(followerMessageHandler);
    }

    private void handleSyncCommands() {
        LeaderMessageHandler followerMessageHandler = new LeaderMessageHandler(new LeaderProxy(lsmTree));
        syncNode.consumeCommand(followerMessageHandler, markHeartbeatAsSuccessful());
    }

    private void handleClientCommandsAsLeader() {
        LeaderMessageHandler leaderMessageHandler = new LeaderMessageHandler(new LeaderProxy(lsmTree));
        clientNode.consumeCommand(leaderMessageHandler, operationRaw -> broadcaster.broadcast(operationRaw));
    }

    private CommandPostHandler markHeartbeatAsSuccessful() {
        return operationRaw -> {
            OperationDto deserialized = OperationDto.deserialize(operationRaw);
            if (deserialized.type().equals(OperationTypeDto.HEARTBEAT)) {
                missingHeartbeatCounter.set(0);
            }
        };
    }

    private void shouldStartElection() {
        int missingHeartbeats = missingHeartbeatCounter.incrementAndGet();
        if (missingHeartbeats >= MAX_MISSING_HEARTBEATS_TO_START_ELECTION) {
            try {
                startElection(missingHeartbeats);
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "No node found to become leader!", e);
                System.exit(1);
            }
        }
    }

    private void startElection(int missingHeartbeats) throws InterruptedException {
        List<NodeAddressWithId> addresses = replicaNodes.stream()
                .sorted(Comparator
                        .comparingInt((ToIntFunction<NodeAddressWithId>) value -> value.address().port())
                        .reversed()
                )
                .toList();

        int skipCandidates = missingHeartbeats / MAX_MISSING_ATTEMPTS_TO_START_RE_ELECTION;
        if (skipCandidates > 0) {
            missingHeartbeatCounter.set(0);
        }
        this.replicaNodes = addresses.stream()
                .skip(skipCandidates)
                .toList();

        if (replicaNodes.size() <= 1) {
            throw new InterruptedException("No node found to become leader!");
        }
        NodeAddressWithId nodeToBeNewLeader = replicaNodes.stream()
                .findFirst()
                .get();

        if (shouldNodeBeNewLeader(nodeToBeNewLeader)) {
            becomeLeader();
            LOGGER.log(Level.INFO,"Node: " + syncAddress.host() + ":" + syncAddress.port() + " became leader.");
        }
    }

    private boolean shouldNodeBeNewLeader(NodeAddressWithId nodeToBeNewLeader) {
        return nodeToBeNewLeader.address().equals(syncAddress);
    }

    private void becomeLeader() {
        executorServices.forEach(ExecutorService::shutdownNow);
        this.broadcaster = new BroadcasterClientCommands(
                replicaNodes.stream()
                        .filter(nodeAddress -> !shouldNodeBeNewLeader(nodeAddress))
                        .toList()
        );

        clientNode.stopConsuming();
        syncNode.stopConsuming();

        this.nodeRole = NodeRole.LEADER;
        this.clientNode = new ClientHandler("leader-node", clientAddress, NodeRole.LEADER, this.leaderExecutor);

        this.start();
    }

    private void sendHeartbeat() {
        broadcaster.broadcast(new OperationDto(OperationTypeDto.HEARTBEAT, "empty").serialize());
    }

}
