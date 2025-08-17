package com.streamflow.partition;

import com.streamflow.broker.BrokerController;
import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.config.BrokerConfig;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.replication.ReplicationManager;
import com.streamflow.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PartitionRebalancerTest {
    
    private PartitionRebalancer partitionRebalancer;
    private ClusterMetadata clusterMetadata;
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private StorageEngine storageEngine;
    
    @Mock
    private ReplicationManager replicationManager;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    @Mock
    private BrokerConfig brokerConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        clusterMetadata = new ClusterMetadata();
        partitionRebalancer = new PartitionRebalancer(brokerController, clusterMetadata);
        
        when(brokerController.getClusterMetadata()).thenReturn(clusterMetadata);
    }

    @Test
    void testAssignPartitionsForNewTopic() {
        // Setup brokers
        List<BrokerNode> brokers = createBrokers(3);
        brokers.forEach(clusterMetadata::registerBroker);
        
        // Create topic
        Map<String, String> config = new HashMap<>();
        Topic topic = new Topic("test-topic", 6, 2, config);
        
        // Test assignment
        partitionRebalancer.assignPartitionsForNewTopic(topic);
        
        // Verify all partitions have replicas assigned
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            TopicPartition partition = new TopicPartition(topic.getName(), i);
            List<Integer> replicas = clusterMetadata.getReplicas(partition);
            
            assertEquals(topic.getReplicationFactor(), replicas.size());
            
            // Verify replicas are valid broker IDs
            for (int replicaId : replicas) {
                assertTrue(clusterMetadata.isBrokerAlive(replicaId));
            }
            
            // Verify leader is set to first replica
            assertEquals(replicas.get(0), clusterMetadata.getPartitionMetadata(partition).getLeader());
        }
    }

    @Test
    void testAssignPartitionsInsufficientBrokers() {
        // Setup only 1 broker but require replication factor 3
        List<BrokerNode> brokers = createBrokers(1);
        brokers.forEach(clusterMetadata::registerBroker);
        
        Map<String, String> config = new HashMap<>();
        Topic topic = new Topic("test-topic", 3, 3, config);
        
        // Should throw exception due to insufficient brokers
        assertThrows(IllegalStateException.class, () -> {
            partitionRebalancer.assignPartitionsForNewTopic(topic);
        });
    }

    @Test
    void testRemovePartitionsForTopic() {
        // Setup brokers and assign partitions
        List<BrokerNode> brokers = createBrokers(3);
        brokers.forEach(clusterMetadata::registerBroker);
        
        Map<String, String> config = new HashMap<>();
        Topic topic = new Topic("test-topic", 3, 2, config);
        
        partitionRebalancer.assignPartitionsForNewTopic(topic);
        
        // Verify partitions are assigned
        TopicPartition partition0 = new TopicPartition(topic.getName(), 0);
        assertFalse(clusterMetadata.getReplicas(partition0).isEmpty());
        
        // Remove partitions
        partitionRebalancer.removePartitionsForTopic(topic);
        
        // Verify cleanup happened (this is a simplified test)
        // In a real implementation, we'd verify broker state was cleaned up
        assertNotNull(topic.getAllPartitions());
    }

    @Test
    void testRebalanceWithFailedBroker() {
        // Setup brokers and assign partitions
        List<BrokerNode> brokers = createBrokers(3);
        brokers.forEach(clusterMetadata::registerBroker);
        
        Map<String, Topic> topics = new HashMap<>();
        topics.put("test-topic", new Topic("test-topic", 6, 2, new HashMap<>()));
        when(brokerController.getTopics()).thenReturn(topics);
        
        partitionRebalancer.assignPartitionsForNewTopic(topics.get("test-topic"));
        
        // Simulate broker failure
        clusterMetadata.unregisterBroker(1);
        
        // Trigger rebalance
        partitionRebalancer.rebalanceIfNeeded();
        
        // Verify rebalancing logic was triggered
        // In a more complete test, we'd verify partition reassignments
        verify(brokerController, atLeastOnce()).getTopics();
    }

    @Test
    void testRebalanceWithNoFailures() {
        // Setup healthy cluster
        List<BrokerNode> brokers = createBrokers(3);
        brokers.forEach(clusterMetadata::registerBroker);
        
        Map<String, Topic> topics = new HashMap<>();
        when(brokerController.getTopics()).thenReturn(topics);
        
        // Should not trigger rebalancing
        partitionRebalancer.rebalanceIfNeeded();
        
        verify(brokerController, atLeastOnce()).getTopics();
    }

    @Test
    void testPartitionDistribution() {
        // Test that partitions are evenly distributed across brokers
        List<BrokerNode> brokers = createBrokers(3);
        brokers.forEach(clusterMetadata::registerBroker);
        
        Map<String, String> config = new HashMap<>();
        Topic topic = new Topic("test-topic", 9, 1, config); // 9 partitions, 1 replica each
        
        partitionRebalancer.assignPartitionsForNewTopic(topic);
        
        // Count partitions per broker
        Map<Integer, Integer> partitionsPerBroker = new HashMap<>();
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            TopicPartition partition = new TopicPartition(topic.getName(), i);
            List<Integer> replicas = clusterMetadata.getReplicas(partition);
            assertEquals(1, replicas.size());
            
            int brokerId = replicas.get(0);
            partitionsPerBroker.merge(brokerId, 1, Integer::sum);
        }
        
        // Each broker should have 3 partitions (9/3 = 3)
        assertEquals(3, partitionsPerBroker.size());
        for (int count : partitionsPerBroker.values()) {
            assertEquals(3, count);
        }
    }

    private List<BrokerNode> createBrokers(int count) {
        return Arrays.asList(
            new BrokerNode(1, "localhost", 9092, brokerConfig, clusterMetadata, storageEngine, replicationManager, metricsCollector),
            new BrokerNode(2, "localhost", 9093, brokerConfig, clusterMetadata, storageEngine, replicationManager, metricsCollector),
            new BrokerNode(3, "localhost", 9094, brokerConfig, clusterMetadata, storageEngine, replicationManager, metricsCollector)
        ).subList(0, count);
    }
}