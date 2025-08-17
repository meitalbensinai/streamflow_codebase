package com.streamflow.partition;

import com.streamflow.broker.BrokerController;
import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.broker.PartitionMetadata;
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
import java.util.Set;

class LeaderElectionTest {
    
    private LeaderElection leaderElection;
    private ClusterMetadata clusterMetadata;
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private BrokerNode brokerNode;
    
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
        leaderElection = new LeaderElection(brokerController, clusterMetadata);
        
        when(brokerController.getBrokerNode()).thenReturn(brokerNode);
        when(brokerNode.getBrokerId()).thenReturn(1);
    }

    @Test
    void testShouldBecomeControllerWhenNoController() {
        // Setup: No current controller
        clusterMetadata.setController(-1); // -1 indicates no controller
        
        // Register brokers
        BrokerNode broker1 = createMockBroker(1);
        BrokerNode broker2 = createMockBroker(2);
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        
        // Broker 1 should become controller (lowest ID)
        when(brokerController.getBrokerNode()).thenReturn(broker1);
        assertTrue(leaderElection.shouldBecomeController());
        
        // Broker 2 should not become controller
        when(brokerController.getBrokerNode()).thenReturn(broker2);
        assertFalse(leaderElection.shouldBecomeController());
    }

    @Test
    void testShouldBecomeControllerWhenCurrentControllerDead() {
        // Setup: Controller exists but is dead
        clusterMetadata.setController(3); // Broker 3 is controller
        
        // Register live brokers (not including broker 3)
        BrokerNode broker1 = createMockBroker(1);
        BrokerNode broker2 = createMockBroker(2);
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        
        // Broker 1 should become controller (lowest ID among alive brokers)
        when(brokerController.getBrokerNode()).thenReturn(broker1);
        assertTrue(leaderElection.shouldBecomeController());
    }

    @Test
    void testShouldNotBecomeControllerWhenControllerAlive() {
        // Setup: Controller exists and is alive
        BrokerNode controller = createMockBroker(1);
        BrokerNode broker2 = createMockBroker(2);
        
        clusterMetadata.registerBroker(controller);
        clusterMetadata.registerBroker(broker2);
        clusterMetadata.setController(1);
        
        // Broker 2 should not become controller since broker 1 is alive
        when(brokerController.getBrokerNode()).thenReturn(broker2);
        assertFalse(leaderElection.shouldBecomeController());
    }

    @Test
    void testIsControllerAlive() {
        // No controller set
        assertFalse(leaderElection.isControllerAlive());
        
        // Controller set but not alive
        clusterMetadata.setController(1);
        assertFalse(leaderElection.isControllerAlive());
        
        // Controller set and alive
        BrokerNode controller = createMockBroker(1);
        clusterMetadata.registerBroker(controller);
        clusterMetadata.setController(1);
        assertTrue(leaderElection.isControllerAlive());
    }

    @Test
    void testElectNewLeaderForPartition() {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        List<Integer> replicas = Arrays.asList(1, 2, 3);
        Set<Integer> isr = Set.of(1, 2, 3);
        
        // Setup brokers
        BrokerNode broker1 = createMockBroker(1);
        BrokerNode broker2 = createMockBroker(2);
        BrokerNode broker3 = createMockBroker(3);
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        clusterMetadata.registerBroker(broker3);
        
        // Setup partition metadata
        clusterMetadata.updatePartitionReplicas(partition, replicas);
        clusterMetadata.updateInSyncReplicas(partition, isr);
        clusterMetadata.updatePartitionLeader(partition, 3); // Broker 3 is current leader
        
        // Simulate broker 3 failure
        clusterMetadata.unregisterBroker(3);
        
        // Elect new leader
        leaderElection.electNewLeaderForPartition(partition);
        
        // Verify new leader was elected from ISR (should be broker 1)
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        int newLeader = metadata.getLeader();
        assertTrue(newLeader == 1 || newLeader == 2); // Should be one of the alive ISR members
    }

    @Test
    void testElectNewLeaderWithNoISRAvailable() {
        TopicPartition partition = new TopicPartition("test-topic", 0);
        List<Integer> replicas = Arrays.asList(1, 2, 3);
        Set<Integer> isr = Set.of(3); // Only broker 3 in ISR
        
        // Setup brokers (but broker 3 is dead)
        BrokerNode broker1 = createMockBroker(1);
        BrokerNode broker2 = createMockBroker(2);
        clusterMetadata.registerBroker(broker1);
        clusterMetadata.registerBroker(broker2);
        
        // Setup partition metadata
        clusterMetadata.updatePartitionReplicas(partition, replicas);
        clusterMetadata.updateInSyncReplicas(partition, isr);
        clusterMetadata.updatePartitionLeader(partition, 3);
        
        // Elect new leader (should perform unclean election)
        leaderElection.electNewLeaderForPartition(partition);
        
        // Should elect one of the alive replicas even though not in ISR
        PartitionMetadata metadata = clusterMetadata.getPartitionMetadata(partition);
        int newLeader = metadata.getLeader();
        assertTrue(newLeader == 1 || newLeader == 2);
    }

    @Test
    void testHandleBrokerFailure() {
        // Setup topics with partitions led by the failing broker
        Map<String, Topic> topics = new HashMap<>();
        topics.put("topic1", new Topic("topic1", 2, 1, new HashMap<>()));
        when(brokerController.getTopics()).thenReturn(topics);
        when(brokerController.getTopic("topic1")).thenReturn(topics.get("topic1"));
        
        // Setup partition metadata where broker 2 is leader
        TopicPartition partition1 = new TopicPartition("topic1", 0);
        TopicPartition partition2 = new TopicPartition("topic1", 1);
        
        clusterMetadata.updatePartitionReplicas(partition1, Arrays.asList(2, 1));
        clusterMetadata.updatePartitionReplicas(partition2, Arrays.asList(1, 2));
        clusterMetadata.updatePartitionLeader(partition1, 2); // Broker 2 leads partition1
        clusterMetadata.updatePartitionLeader(partition2, 1); // Broker 1 leads partition2
        
        // Register alive broker
        BrokerNode broker1 = createMockBroker(1);
        clusterMetadata.registerBroker(broker1);
        
        // Handle broker 2 failure
        leaderElection.handleBrokerFailure(2);
        
        // Verify that leader election was triggered for partition1
        // (This test is simplified - in reality we'd verify specific elections occurred)
        verify(brokerController, atLeastOnce()).getTopics();
    }

    @Test
    void testHandleBrokerRecovery() {
        // Test that broker recovery is handled gracefully
        assertDoesNotThrow(() -> {
            leaderElection.handleBrokerRecovery(1);
        });
    }

    private BrokerNode createMockBroker(int id) {
        return new BrokerNode(id, "localhost", 9090 + id, brokerConfig, 
                             storageEngine, replicationManager, metricsCollector);
    }
}