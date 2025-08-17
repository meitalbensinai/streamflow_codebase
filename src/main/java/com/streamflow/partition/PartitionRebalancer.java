package com.streamflow.partition;

import com.streamflow.broker.BrokerController;
import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PartitionRebalancer {
    private static final Logger logger = LoggerFactory.getLogger(PartitionRebalancer.class);
    
    private final BrokerController controller;
    private final ClusterMetadata clusterMetadata;
    private final PartitionAssignmentStrategy assignmentStrategy;

    public PartitionRebalancer(BrokerController controller, ClusterMetadata clusterMetadata) {
        this.controller = controller;
        this.clusterMetadata = clusterMetadata;
        this.assignmentStrategy = new RoundRobinAssignmentStrategy();
    }

    public void assignPartitionsForNewTopic(Topic topic) {
        logger.info("Assigning partitions for new topic: {}", topic.getName());
        
        List<BrokerNode> availableBrokers = clusterMetadata.getAliveBrokers();
        if (availableBrokers.size() < topic.getReplicationFactor()) {
            throw new IllegalStateException(
                "Not enough brokers available. Required: " + topic.getReplicationFactor() + 
                ", Available: " + availableBrokers.size()
            );
        }

        Map<TopicPartition, List<Integer>> assignments = assignmentStrategy.assign(
            topic, availableBrokers
        );

        for (Map.Entry<TopicPartition, List<Integer>> entry : assignments.entrySet()) {
            TopicPartition partition = entry.getKey();
            List<Integer> replicas = entry.getValue();
            
            clusterMetadata.updatePartitionReplicas(partition, replicas);
            clusterMetadata.updatePartitionLeader(partition, replicas.get(0));
            
            // Notify brokers about their new roles
            for (int i = 0; i < replicas.size(); i++) {
                int brokerId = replicas.get(i);
                BrokerNode broker = clusterMetadata.getBroker(brokerId);
                if (broker != null) {
                    if (i == 0) {
                        broker.becomeLeaderFor(partition);
                    } else {
                        broker.becomeFollowerFor(partition);
                    }
                }
            }
        }
        
        logger.info("Completed partition assignment for topic: {}", topic.getName());
    }

    public void removePartitionsForTopic(Topic topic) {
        logger.info("Removing partitions for topic: {}", topic.getName());
        
        for (TopicPartition partition : topic.getAllPartitions()) {
            List<Integer> replicas = clusterMetadata.getReplicas(partition);
            
            for (int brokerId : replicas) {
                BrokerNode broker = clusterMetadata.getBroker(brokerId);
                if (broker != null) {
                    // Remove partition from broker's assignments
                    broker.becomeFollowerFor(partition); // This removes from leader set
                    // Additional cleanup would happen here
                }
            }
        }
        
        logger.info("Completed partition removal for topic: {}", topic.getName());
    }

    public void rebalanceIfNeeded() {
        logger.debug("Checking if rebalancing is needed");
        
        List<BrokerNode> aliveBrokers = clusterMetadata.getAliveBrokers();
        if (aliveBrokers.isEmpty()) {
            logger.warn("No alive brokers available for rebalancing");
            return;
        }

        Map<String, Topic> topics = controller.getTopics();
        boolean rebalanceNeeded = false;
        
        for (Topic topic : topics.values()) {
            if (isTopicImbalanced(topic, aliveBrokers)) {
                rebalanceNeeded = true;
                break;
            }
        }

        if (rebalanceNeeded) {
            performRebalance(topics.values(), aliveBrokers);
        }
    }

    private boolean isTopicImbalanced(Topic topic, List<BrokerNode> aliveBrokers) {
        // Check if any partition has replicas on failed brokers
        for (TopicPartition partition : topic.getAllPartitions()) {
            List<Integer> replicas = clusterMetadata.getReplicas(partition);
            for (int replicaBrokerId : replicas) {
                if (!clusterMetadata.isBrokerAlive(replicaBrokerId)) {
                    return true;
                }
            }
            
            // Check if ISR is too small
            Set<Integer> isr = clusterMetadata.getInSyncReplicas(partition);
            if (isr.size() < Math.min(topic.getReplicationFactor(), aliveBrokers.size())) {
                return true;
            }
        }
        return false;
    }

    private void performRebalance(Collection<Topic> topics, List<BrokerNode> aliveBrokers) {
        logger.info("Performing cluster rebalance");
        
        for (Topic topic : topics) {
            rebalanceTopic(topic, aliveBrokers);
        }
        
        logger.info("Cluster rebalance completed");
    }

    private void rebalanceTopic(Topic topic, List<BrokerNode> aliveBrokers) {
        logger.info("Rebalancing topic: {}", topic.getName());
        
        // Generate new assignment
        Map<TopicPartition, List<Integer>> newAssignments = assignmentStrategy.assign(
            topic, aliveBrokers
        );

        // Compare with current assignments and perform minimal moves
        for (Map.Entry<TopicPartition, List<Integer>> entry : newAssignments.entrySet()) {
            TopicPartition partition = entry.getKey();
            List<Integer> newReplicas = entry.getValue();
            List<Integer> currentReplicas = clusterMetadata.getReplicas(partition);

            if (!newReplicas.equals(currentReplicas)) {
                migratePartition(partition, currentReplicas, newReplicas);
            }
        }
    }

    private void migratePartition(TopicPartition partition, List<Integer> oldReplicas, 
                                 List<Integer> newReplicas) {
        logger.info("Migrating partition {} from {} to {}", partition, oldReplicas, newReplicas);
        
        // Step 1: Add new replicas and start replication
        for (int newReplicaId : newReplicas) {
            if (!oldReplicas.contains(newReplicaId)) {
                BrokerNode broker = clusterMetadata.getBroker(newReplicaId);
                if (broker != null) {
                    broker.becomeFollowerFor(partition);
                    // Start replication from leader
                }
            }
        }

        // Step 2: Wait for new replicas to catch up (simplified)
        // In real implementation, this would involve monitoring replication lag

        // Step 3: Update metadata
        clusterMetadata.updatePartitionReplicas(partition, newReplicas);
        
        // Step 4: Remove old replicas
        for (int oldReplicaId : oldReplicas) {
            if (!newReplicas.contains(oldReplicaId)) {
                BrokerNode broker = clusterMetadata.getBroker(oldReplicaId);
                if (broker != null) {
                    // Remove partition data and stop serving
                }
            }
        }

        // Step 5: Update leader if necessary
        int newLeader = newReplicas.get(0);
        clusterMetadata.updatePartitionLeader(partition, newLeader);
        
        BrokerNode leaderBroker = clusterMetadata.getBroker(newLeader);
        if (leaderBroker != null) {
            leaderBroker.becomeLeaderFor(partition);
        }
        
        logger.info("Completed migration of partition {}", partition);
    }
}