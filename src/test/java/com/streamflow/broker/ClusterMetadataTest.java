package com.streamflow.broker;

import com.streamflow.config.BrokerConfig;
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
import java.util.List;
import java.util.Set;

class ClusterMetadataTest {
    
    private ClusterMetadata clusterMetadata;
    
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
    }

    @Test
    void testBrokerRegistration() {
        BrokerNode broker1 = createMockBroker(1, "localhost", 9092);
        BrokerNode broker2 = createMockBroker(2, "localhost", 9093);
        
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        
        assertEquals(2, clusterMetadata.getAliveBrokerCount());
        assertTrue(clusterMetadata.isBrokerAlive(1));
        assertTrue(clusterMetadata.isBrokerAlive(2));
        assertFalse(clusterMetadata.isBrokerAlive(3));
        
        assertEquals(broker1, clusterMetadata.getBroker(1));
        assertEquals(broker2, clusterMetadata.getBroker(2));
    }

    @Test
    void testBrokerUnregistration() {
        BrokerNode broker = createMockBroker(1, "localhost", 9092);
        clusterMetadata.registerBroker(broker);
        
        assertTrue(clusterMetadata.isBrokerAlive(1));
        assertEquals(1, clusterMetadata.getAliveBrokerCount());
        
        clusterMetadata.unregisterBroker(1);
        
        assertFalse(clusterMetadata.isBrokerAlive(1));
        assertEquals(0, clusterMetadata.getAliveBrokerCount());
        assertNull(clusterMetadata.getBroker(1));
    }

    @Test
    void testPartitionMetadataManagement() {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        List<Integer> replicas = Arrays.asList(1, 2, 3);
        
        clusterMetadata.updatePartitionReplicas(partition, replicas);
        
        assertEquals(replicas, clusterMetadata.getReplicas(partition));
        assertEquals(Set.of(1, 2, 3), clusterMetadata.getInSyncReplicas(partition));
        
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        assertNotNull(metadata);
        assertEquals(partition, metadata.getTopicPartition());
        assertEquals(1, metadata.getLeader()); // First replica becomes leader
    }

    @Test
    void testPartitionLeaderUpdate() {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        List<Integer> replicas = Arrays.asList(1, 2, 3);
        
        clusterMetadata.updatePartitionReplicas(partition, replicas);
        clusterMetadata.updatePartitionLeader(partition, 2);
        
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        assertEquals(2, metadata.getLeader());
    }

    @Test
    void testInSyncReplicasUpdate() {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        List<Integer> replicas = Arrays.asList(1, 2, 3);
        Set<Integer> newIsr = Set.of(1, 2);
        
        clusterMetadata.updatePartitionReplicas(partition, replicas);
        clusterMetadata.updateInSyncReplicas(partition, newIsr);
        
        assertEquals(newIsr, clusterMetadata.getInSyncReplicas(partition));
        
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        assertTrue(metadata.getInSyncReplicas().containsAll(List.of(1, 2)) && 
                   metadata.getInSyncReplicas().size() == 2);
    }

    @Test
    void testControllerManagement() {
        assertNull(clusterMetadata.getController());
        
        clusterMetadata.setController(1);
        assertEquals(Integer.valueOf(1), clusterMetadata.getController());
        
        clusterMetadata.setController(2);
        assertEquals(Integer.valueOf(2), clusterMetadata.getController());
    }

    @Test
    void testGetAliveBrokers() {
        BrokerNode broker1 = createMockBroker(1, "localhost", 9092);
        BrokerNode broker2 = createMockBroker(2, "localhost", 9093);
        
        assertTrue(clusterMetadata.getAliveBrokers().isEmpty());
        
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        
        List<BrokerNode> aliveBrokers = clusterMetadata.getAliveBrokers();
        assertEquals(2, aliveBrokers.size());
        assertTrue(aliveBrokers.contains(broker1));
        assertTrue(aliveBrokers.contains(broker2));
    }

    @Test
    void testConcurrentBrokerOperations() {
        // Test thread safety by registering/unregistering brokers concurrently
        BrokerNode broker1 = createMockBroker(1, "localhost", 9092);
        BrokerNode broker2 = createMockBroker(2, "localhost", 9093);
        
        // This test would be enhanced with actual concurrent operations
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        clusterMetadata.unregisterBroker(1);
        
        assertEquals(1, clusterMetadata.getAliveBrokerCount());
        assertTrue(clusterMetadata.isBrokerAlive(2));
        assertFalse(clusterMetadata.isBrokerAlive(1));
    }

    private BrokerNode createMockBroker(int id, String host, int port) {
        return new BrokerNode(id, host, port, brokerConfig, storageEngine, 
                             replicationManager, metricsCollector);
    }
}