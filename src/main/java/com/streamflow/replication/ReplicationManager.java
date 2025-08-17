package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    private final Map<TopicPartition, ReplicaManager> replicaManagers;
    private final Map<TopicPartition, ReplicationFetcher> replicationFetchers;
    private final ScheduledExecutorService executorService;
    private BrokerNode localBroker;

    public ReplicationManager() {
        this.replicaManagers = new ConcurrentHashMap<>();
        this.replicationFetchers = new ConcurrentHashMap<>();
        this.executorService = Executors.newScheduledThreadPool(4);
    }

    public void initialize(BrokerNode broker) {
        this.localBroker = broker;
        logger.info("Initialized replication manager for broker {}", broker.getBrokerId());
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Replication manager shutdown completed");
    }

    public void becomeLeaderForPartition(TopicPartition partition, List<Integer> replicaIds) {
        logger.info("Becoming leader for partition {} with replicas {}", partition, replicaIds);
        
        // Stop any existing fetcher for this partition
        ReplicationFetcher fetcher = replicationFetchers.remove(partition);
        if (fetcher != null) {
            fetcher.stop();
        }

        // Create replica manager for leader duties
        ReplicaManager replicaManager = new ReplicaManager(partition, replicaIds, localBroker);
        replicaManagers.put(partition, replicaManager);
        replicaManager.start();
    }

    public void becomeFollowerForPartition(TopicPartition partition, int leaderId) {
        logger.info("Becoming follower for partition {} with leader {}", partition, leaderId);
        
        // Stop any existing replica manager
        ReplicaManager replicaManager = replicaManagers.remove(partition);
        if (replicaManager != null) {
            replicaManager.stop();
        }

        // Start fetching from leader
        ReplicationFetcher fetcher = new ReplicationFetcher(partition, leaderId, localBroker);
        replicationFetchers.put(partition, fetcher);
        fetcher.start();
    }

    public void removePartition(TopicPartition partition) {
        logger.info("Removing partition {} from replication", partition);
        
        ReplicaManager replicaManager = replicaManagers.remove(partition);
        if (replicaManager != null) {
            replicaManager.stop();
        }

        ReplicationFetcher fetcher = replicationFetchers.remove(partition);
        if (fetcher != null) {
            fetcher.stop();
        }
        
        logger.info("Stopped replication for partition {}", partition);
    }

    public void replicateMessage(TopicPartition partition, Message message) {
        ReplicaManager replicaManager = replicaManagers.get(partition);
        if (replicaManager != null) {
            replicaManager.replicateMessage(message);
        } else {
            logger.warn("No replica manager found for partition {} when trying to replicate message", partition);
        }
    }

    public long getHighWatermark(TopicPartition partition) {
        ReplicaManager replicaManager = replicaManagers.get(partition);
        if (replicaManager != null) {
            return replicaManager.getHighWatermark();
        }
        return 0L;
    }

    public boolean isInSyncReplica(TopicPartition partition, int brokerId) {
        ReplicaManager replicaManager = replicaManagers.get(partition);
        if (replicaManager != null) {
            return replicaManager.isInSyncReplica(brokerId);
        }
        return false;
    }

    public void updateReplicaLag(TopicPartition partition, int replicaId, long lag) {
        ReplicaManager replicaManager = replicaManagers.get(partition);
        if (replicaManager != null) {
            replicaManager.updateReplicaLag(replicaId, lag);
        }
    }

    public Map<Integer, Long> getReplicaLags(TopicPartition partition) {
        ReplicaManager replicaManager = replicaManagers.get(partition);
        if (replicaManager != null) {
            return replicaManager.getReplicaLags();
        }
        return Map.of();
    }

    public void handleBrokerFailure(int failedBrokerId) {
        logger.warn("Handling broker failure: {}", failedBrokerId);
        
        // Update all replica managers about the failed broker
        for (ReplicaManager replicaManager : replicaManagers.values()) {
            replicaManager.handleReplicaFailure(failedBrokerId);
        }

        // Update fetchers if they were fetching from the failed broker
        for (Map.Entry<TopicPartition, ReplicationFetcher> entry : replicationFetchers.entrySet()) {
            ReplicationFetcher fetcher = entry.getValue();
            if (fetcher.getLeaderId() == failedBrokerId) {
                fetcher.handleLeaderFailure();
            }
        }
    }

    public void handleBrokerRecovery(int recoveredBrokerId) {
        logger.info("Handling broker recovery: {}", recoveredBrokerId);
        
        // Notify replica managers about the recovered broker
        for (ReplicaManager replicaManager : replicaManagers.values()) {
            replicaManager.handleReplicaRecovery(recoveredBrokerId);
        }
    }

    // Health monitoring
    public void startHealthMonitoring() {
        executorService.scheduleAtFixedRate(this::checkReplicationHealth, 30, 30, TimeUnit.SECONDS);
    }

    private void checkReplicationHealth() {
        try {
            for (Map.Entry<TopicPartition, ReplicaManager> entry : replicaManagers.entrySet()) {
                TopicPartition partition = entry.getKey();
                ReplicaManager replicaManager = entry.getValue();
                
                Map<Integer, Long> lags = replicaManager.getReplicaLags();
                for (Map.Entry<Integer, Long> lagEntry : lags.entrySet()) {
                    int replicaId = lagEntry.getKey();
                    long lag = lagEntry.getValue();
                    
                    if (lag > localBroker.getConfig().getReplicaLagTimeMaxMs()) {
                        logger.warn("Replica {} for partition {} is lagging by {} ms", 
                                   replicaId, partition, lag);
                        replicaManager.removeFromISR(replicaId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error during replication health check", e);
        }
    }
}