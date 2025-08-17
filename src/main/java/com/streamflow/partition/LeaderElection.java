package com.streamflow.partition;

import com.streamflow.broker.BrokerController;
import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.broker.PartitionMetadata;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class LeaderElection {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    
    private final BrokerController controller;
    private final ClusterMetadata clusterMetadata;

    public LeaderElection(BrokerController controller, ClusterMetadata clusterMetadata) {
        this.controller = controller;
        this.clusterMetadata = clusterMetadata;
    }

    public boolean shouldBecomeController() {
        Integer currentController = clusterMetadata.getController();
        
        // If no controller or current controller is dead
        if (currentController == null || !clusterMetadata.isBrokerAlive(currentController)) {
            // Simple election: lowest broker ID becomes controller
            List<BrokerNode> aliveBrokers = clusterMetadata.getAliveBrokers();
            if (!aliveBrokers.isEmpty()) {
                int lowestId = aliveBrokers.stream()
                    .mapToInt(BrokerNode::getBrokerId)
                    .min()
                    .orElse(Integer.MAX_VALUE);
                
                return controller.getBrokerId() == lowestId;
            }
        }
        
        return false;
    }

    public boolean isControllerAlive() {
        Integer controllerId = clusterMetadata.getController();
        return controllerId != null && clusterMetadata.isBrokerAlive(controllerId);
    }

    public void electNewLeaderForPartition(TopicPartition partition) {
        logger.info("Electing new leader for partition: {}", partition);
        
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        if (metadata == null) {
            logger.warn("No metadata found for partition: {}", partition);
            return;
        }

        List<Integer> replicas = clusterMetadata.getReplicas(partition);
        Set<Integer> isr = clusterMetadata.getInSyncReplicas(partition);
        
        // Find the first alive replica in ISR
        Integer newLeader = null;
        for (int replicaId : replicas) {
            if (isr.contains(replicaId) && clusterMetadata.isBrokerAlive(replicaId)) {
                newLeader = replicaId;
                break;
            }
        }

        if (newLeader == null) {
            // If no ISR replica is available, find any alive replica (unclean election)
            logger.warn("No ISR replica available for partition {}, performing unclean leader election", partition);
            for (int replicaId : replicas) {
                if (clusterMetadata.isBrokerAlive(replicaId)) {
                    newLeader = replicaId;
                    break;
                }
            }
        }

        if (newLeader != null && newLeader != metadata.getLeader()) {
            // Demote old leader
            BrokerNode oldLeader = clusterMetadata.getBroker(metadata.getLeader());
            if (oldLeader != null) {
                oldLeader.becomeFollowerFor(partition);
            }

            // Promote new leader
            BrokerNode newLeaderBroker = clusterMetadata.getBroker(newLeader);
            if (newLeaderBroker != null) {
                newLeaderBroker.becomeLeaderFor(partition);
                clusterMetadata.updatePartitionLeader(partition, newLeader);
                logger.info("Elected broker {} as new leader for partition {}", newLeader, partition);
            }
        } else if (newLeader == null) {
            logger.error("No available replica found for partition: {}", partition);
        }
    }

    public void handleBrokerFailure(int failedBrokerId) {
        logger.warn("Handling failure of broker: {}", failedBrokerId);
        
        // Find all partitions where this broker was leader
        for (String topicName : controller.getTopics().keySet()) {
            for (TopicPartition partition : controller.getTopic(topicName).getAllPartitions()) {
                PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
                if (metadata != null && metadata.getLeader() == failedBrokerId) {
                    electNewLeaderForPartition(partition);
                }
            }
        }
    }

    public void handleBrokerRecovery(int recoveredBrokerId) {
        logger.info("Handling recovery of broker: {}", recoveredBrokerId);
        
        // In a real implementation, this might trigger rebalancing
        // to restore the preferred replica leadership
    }
}