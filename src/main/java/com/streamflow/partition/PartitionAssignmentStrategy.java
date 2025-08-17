package com.streamflow.partition;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;

import java.util.List;
import java.util.Map;

public interface PartitionAssignmentStrategy {
    Map<TopicPartition, List<Integer>> assign(Topic topic, List<BrokerNode> availableBrokers);
}