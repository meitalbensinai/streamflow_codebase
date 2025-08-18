package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaManager.class);
    
    private final TopicPartition partition;
    private final List<Integer> allReplicas;
    private final Set<Integer> inSyncReplicas;
    private final Map<Integer, AtomicLong> replicaLags;
    protected final BrokerNode localBroker;
    private final AtomicLong highWatermark;
    private volatile boolean isRunning;

    public ReplicaManager(TopicPartition partition, List<Integer> allReplicas, BrokerNode localBroker) {
        this.partition = partition;
        this.allReplicas = allReplicas;
        this.inSyncReplicas = new CopyOnWriteArraySet<>(allReplicas);
        this.replicaLags = new ConcurrentHashMap<>();
        this.localBroker = localBroker;
        this.highWatermark = new AtomicLong(0);
        this.isRunning = false;
        
        // Initialize replica lags
        for (int replicaId : allReplicas) {
            replicaLags.put(replicaId, new AtomicLong(0));
        }
    }

    public void start() {
        isRunning = true;
        logger.info("Started replica manager for partition {}", partition);
    }

    public void stop() {
        isRunning = false;
        logger.info("Stopped replica manager for partition {}", partition);
    }

    public void replicateMessage(Message message) {
        if (!isRunning) {
            logger.warn("Replica manager not running for partition {}", partition);
            return;
        }

        // Send message to all replicas (simplified)
        for (int replicaId : allReplicas) {
            if (replicaId != localBroker.getBrokerId()) {
                sendToReplica(replicaId, message);
            }
        }
        
        // Update high watermark based on ISR
        updateHighWatermark();
    }

    private void sendToReplica(int replicaId, Message message) {
        // In a real implementation, this would send over network
        logger.debug("Sending message to replica {} for partition {}", replicaId, partition);
    }

    public void updateReplicaLag(int replicaId, long lag) {
        AtomicLong replicaLag = replicaLags.get(replicaId);
        if (replicaLag != null) {
            replicaLag.set(lag);
            
            // Check if replica should be removed from ISR
            long maxLag = localBroker.getConfig().getReplicaLagTimeMaxMs();
            if (lag > maxLag) {
                removeFromISR(replicaId);
            } else if (!inSyncReplicas.contains(replicaId)) {
                // Replica caught up, add back to ISR
                addToISR(replicaId);
            }
        }
    }

    public void removeFromISR(int replicaId) {
        if (inSyncReplicas.remove(replicaId)) {
            logger.warn("Removed replica {} from ISR for partition {}", replicaId, partition);
            updateHighWatermark();
            
            // Update cluster metadata
            localBroker.getController().getClusterMetadata()
                .updateInSyncReplicas(partition, inSyncReplicas);
        }
    }

    public void addToISR(int replicaId) {
        if (allReplicas.contains(replicaId) && inSyncReplicas.add(replicaId)) {
            logger.info("Added replica {} to ISR for partition {}", replicaId, partition);
            updateHighWatermark();
            
            // Update cluster metadata
            localBroker.getController().getClusterMetadata()
                .updateInSyncReplicas(partition, inSyncReplicas);
        }
    }

    protected void updateHighWatermark() {
        // High watermark is the minimum offset that all ISR replicas have
        // For simplicity, we'll just increment it
        long newHighWatermark = calculateHighWatermark();
        highWatermark.set(newHighWatermark);
        
        logger.debug("Updated high watermark for partition {} to {}", partition, newHighWatermark);
    }

    private long calculateHighWatermark() {
        // In a real implementation, this would check the actual offsets of ISR replicas
        try {
            if (localBroker != null && localBroker.getStorageEngine() != null) {
                return localBroker.getStorageEngine().getLogEndOffset(partition);
            }
        } catch (Exception e) {
            // In case of any error, return current high watermark
            logger.debug("Error calculating high watermark, using current value", e);
        }
        return highWatermark.get();
    }

    public void handleReplicaFailure(int failedReplicaId) {
        logger.warn("Handling failure of replica {} for partition {}", failedReplicaId, partition);
        removeFromISR(failedReplicaId);
    }

    public void handleReplicaRecovery(int recoveredReplicaId) {
        logger.info("Handling recovery of replica {} for partition {}", recoveredReplicaId, partition);
        
        // Don't automatically add to ISR - replica needs to catch up first
        if (allReplicas.contains(recoveredReplicaId)) {
            replicaLags.put(recoveredReplicaId, new AtomicLong(Long.MAX_VALUE));
        }
    }

    public boolean isInSyncReplica(int replicaId) {
        return inSyncReplicas.contains(replicaId);
    }

    public long getHighWatermark() {
        return highWatermark.get();
    }

    public Map<Integer, Long> getReplicaLags() {
        Map<Integer, Long> lags = new ConcurrentHashMap<>();
        replicaLags.forEach((replicaId, lag) -> lags.put(replicaId, lag.get()));
        return lags;
    }

    public Set<Integer> getInSyncReplicas() {
        return Set.copyOf(inSyncReplicas);
    }

    public List<Integer> getAllReplicas() {
        return List.copyOf(allReplicas);
    }

    public TopicPartition getPartition() {
        return partition;
    }

    public boolean isRunning() {
        return isRunning;
    }
}