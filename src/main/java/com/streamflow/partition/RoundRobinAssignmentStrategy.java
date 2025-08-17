package com.streamflow.partition;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RoundRobinAssignmentStrategy implements PartitionAssignmentStrategy {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinAssignmentStrategy.class);

    @Override
    public Map<TopicPartition, List<Integer>> assign(Topic topic, List<BrokerNode> availableBrokers) {
        logger.debug("Assigning partitions for topic {} using round-robin strategy", topic.getName());
        
        Map<TopicPartition, List<Integer>> assignments = new HashMap<>();
        List<Integer> brokerIds = availableBrokers.stream()
            .map(BrokerNode::getBrokerId)
            .sorted()
            .collect(java.util.stream.Collectors.toList());

        int replicationFactor = Math.min(topic.getReplicationFactor(), brokerIds.size());
        
        for (int partitionId = 0; partitionId < topic.getPartitionCount(); partitionId++) {
            TopicPartition partition = new TopicPartition(topic.getName(), partitionId);
            List<Integer> replicas = new ArrayList<>();
            
            // Select replicas using round-robin with offset
            for (int replicaIndex = 0; replicaIndex < replicationFactor; replicaIndex++) {
                int brokerIndex = (partitionId + replicaIndex) % brokerIds.size();
                replicas.add(brokerIds.get(brokerIndex));
            }
            
            assignments.put(partition, replicas);
            logger.debug("Assigned partition {} to replicas {}", partition, replicas);
        }
        
        return assignments;
    }
}