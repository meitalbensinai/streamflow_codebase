package com.streamflow.broker;

import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import com.streamflow.replication.ReplicationManager;
import com.streamflow.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    
    private final BrokerNode broker;
    private final StorageEngine storageEngine;
    private final ReplicationManager replicationManager;
    private final Map<TopicPartition, PartitionState> partitionStates;

    public PartitionManager(BrokerNode broker, StorageEngine storageEngine, 
                           ReplicationManager replicationManager) {
        this.broker = broker;
        this.storageEngine = storageEngine;
        this.replicationManager = replicationManager;
        this.partitionStates = new ConcurrentHashMap<>();
    }

    public void startup() {
        logger.info("Starting partition manager for broker {}", broker.getBrokerId());
    }

    public void shutdown() {
        logger.info("Shutting down partition manager for broker {}", broker.getBrokerId());
    }

    public void promoteToLeader(TopicPartition partition) {
        logger.info("Promoting partition {} to leader on broker {}", partition, broker.getBrokerId());
        
        PartitionState state = partitionStates.computeIfAbsent(partition, 
            p -> new PartitionState(p, PartitionRole.FOLLOWER));
        
        state.setRole(PartitionRole.LEADER);
        
        // Get replica list from cluster metadata
        // replicationManager.becomeLeaderForPartition(partition, replicas);
    }

    public void demoteToFollower(TopicPartition partition) {
        logger.info("Demoting partition {} to follower on broker {}", partition, broker.getBrokerId());
        
        PartitionState state = partitionStates.get(partition);
        if (state != null) {
            state.setRole(PartitionRole.FOLLOWER);
            // replicationManager.becomeFollowerForPartition(partition, leaderId);
        }
    }

    public long appendMessage(TopicPartition partition, Message message) {
        PartitionState state = partitionStates.get(partition);
        if (state == null || state.getRole() != PartitionRole.LEADER) {
            throw new IllegalStateException("Cannot append to partition " + partition + 
                                          " - not a leader");
        }

        long offset = storageEngine.append(partition, message);
        replicationManager.replicateMessage(partition, message);
        
        return offset;
    }

    private enum PartitionRole {
        LEADER, FOLLOWER
    }

    private static class PartitionState {
        private final TopicPartition partition;
        private volatile PartitionRole role;

        public PartitionState(TopicPartition partition, PartitionRole role) {
            this.partition = partition;
            this.role = role;
        }

        public TopicPartition getPartition() { return partition; }
        public PartitionRole getRole() { return role; }
        public void setRole(PartitionRole role) { this.role = role; }
    }
}