package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.BrokerController;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.config.BrokerConfig;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.storage.StorageEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

public class BatchingReplicationManagerTest {
    
    @Mock
    private BrokerNode mockBroker;
    
    @Mock
    private MetricsCollector mockMetrics;
    
    @Mock
    private StorageEngine mockStorageEngine;
    
    @Mock
    private BrokerController mockController;
    
    @Mock
    private ClusterMetadata mockClusterMetadata;
    
    private BatchingReplicationManager batchingManager;
    private BrokerConfig config;
    private TopicPartition partition;
    
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Create config with batching enabled
        Properties props = new Properties();
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.REPLICATION_BATCHING_ENABLED, "true");
        props.setProperty(BrokerConfig.REPLICATION_BATCH_SIZE, "5");
        props.setProperty(BrokerConfig.REPLICATION_BATCH_TIMEOUT_MS, "100");
        props.setProperty(BrokerConfig.REPLICATION_BATCH_MAX_BYTES, "1024");
        
        config = new BrokerConfig(props);
        when(mockBroker.getConfig()).thenReturn(config);
        when(mockBroker.getBrokerId()).thenReturn(1);
        when(mockBroker.getStorageEngine()).thenReturn(mockStorageEngine);
        when(mockBroker.getController()).thenReturn(mockController);
        when(mockController.getClusterMetadata()).thenReturn(mockClusterMetadata);
        when(mockStorageEngine.getLogEndOffset(any())).thenReturn(100L);
        
        batchingManager = new BatchingReplicationManager(mockMetrics);
        batchingManager.initialize(mockBroker);
        
        partition = new TopicPartition("test-topic", 0);
    }
    
    @Test
    public void testInitializationWithBatchingEnabled() {
        assertTrue(config.isReplicationBatchingEnabled());
        assertEquals(5, config.getReplicationBatchSize());
        assertEquals(100, config.getReplicationBatchTimeoutMs());
        assertEquals(1024, config.getReplicationBatchMaxBytes());
    }
    
    @Test
    public void testInitializationWithBatchingDisabled() {
        Properties props = new Properties();
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.REPLICATION_BATCHING_ENABLED, "false");
        
        BrokerConfig disabledConfig = new BrokerConfig(props);
        when(mockBroker.getConfig()).thenReturn(disabledConfig);
        
        BatchingReplicationManager disabledManager = new BatchingReplicationManager(mockMetrics);
        disabledManager.initialize(mockBroker);
        
        assertFalse(disabledConfig.isReplicationBatchingEnabled());
        
        disabledManager.shutdown();
    }
    
    @Test
    public void testBecomeLeaderForPartition() {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Verify metrics were called for batch creation when messages are added
        // Note: Actual metrics calls happen when messages are replicated
        verify(mockBroker, atLeastOnce()).getConfig();
    }
    
    @Test
    public void testReplicateMessageAsync() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> future = batchingManager.replicateMessageAsync(partition, message);
        
        // The future should complete (though it may take some time due to batching)
        assertNotNull(future);
        
        // Add more messages to trigger batch flush
        for (int i = 2; i <= 5; i++) {
            Message msg = createTestMessage("key" + i, "value" + i);
            batchingManager.replicateMessageAsync(partition, msg);
        }
        
        // Wait for completion
        assertDoesNotThrow(() -> future.get(2, TimeUnit.SECONDS));
    }
    
    @Test
    public void testBatchFlushOnSizeLimit() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Add exactly batch size number of messages
        CompletableFuture<Void> lastFuture = null;
        for (int i = 1; i <= 5; i++) {
            Message message = createTestMessage("key" + i, "value" + i);
            lastFuture = batchingManager.replicateMessageAsync(partition, message);
        }
        
        // The batch should flush automatically when size limit is reached
        assertNotNull(lastFuture);
        final CompletableFuture<Void> finalLastFuture = lastFuture;
        assertDoesNotThrow(() -> finalLastFuture.get(2, TimeUnit.SECONDS));
        
        // Verify batch metrics were recorded
        verify(mockMetrics, atLeastOnce()).recordBatchCreated(partition);
    }
    
    @Test
    public void testBatchFlushOnTimeout() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Add a single message (less than batch size)
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> future = batchingManager.replicateMessageAsync(partition, message);
        
        // Wait for timeout-based flush (should happen within 100ms + some buffer)
        assertDoesNotThrow(() -> future.get(1, TimeUnit.SECONDS));
        
        // Verify timeout metrics might be recorded
        verify(mockMetrics, atLeastOnce()).recordBatchCreated(partition);
    }
    
    @Test
    public void testRemovePartitionFlushesExistingBatch() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Add a message but don't fill the batch
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> future = batchingManager.replicateMessageAsync(partition, message);
        
        // Remove partition should flush existing batch
        batchingManager.removePartition(partition);
        
        // The future should complete due to partition removal flush
        assertDoesNotThrow(() -> future.get(1, TimeUnit.SECONDS));
    }
    
    @Test
    public void testBecomeFollowerFlushesExistingBatch() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Add a message but don't fill the batch
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> future = batchingManager.replicateMessageAsync(partition, message);
        
        // Become follower should flush existing batch
        batchingManager.becomeFollowerForPartition(partition, 2);
        
        // The future should complete due to role change flush
        assertDoesNotThrow(() -> future.get(1, TimeUnit.SECONDS));
    }
    
    @Test
    public void testShutdownFlushesAllBatches() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Add messages to multiple partitions
        TopicPartition partition2 = new TopicPartition("test-topic", 1);
        batchingManager.becomeLeaderForPartition(partition2, Arrays.asList(1, 2, 3));
        
        CompletableFuture<Void> future1 = batchingManager.replicateMessageAsync(
            partition, createTestMessage("key1", "value1"));
        CompletableFuture<Void> future2 = batchingManager.replicateMessageAsync(
            partition2, createTestMessage("key2", "value2"));
        
        // Shutdown should flush all remaining batches
        batchingManager.shutdown();
        
        // Both futures should complete due to shutdown flush
        assertDoesNotThrow(() -> future1.get(1, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> future2.get(1, TimeUnit.SECONDS));
    }
    
    @Test
    public void testFallbackToRegularReplicationWhenBatchingDisabled() {
        // Create config with batching disabled
        Properties props = new Properties();
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.REPLICATION_BATCHING_ENABLED, "false");
        
        BrokerConfig disabledConfig = new BrokerConfig(props);
        when(mockBroker.getConfig()).thenReturn(disabledConfig);
        
        BatchingReplicationManager disabledManager = new BatchingReplicationManager(mockMetrics);
        disabledManager.initialize(mockBroker);
        
        disabledManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Should fall back to regular replication (no exception should be thrown)
        Message message = createTestMessage("key1", "value1");
        assertDoesNotThrow(() -> disabledManager.replicateMessage(partition, message));
        
        disabledManager.shutdown();
    }
    
    @Test
    public void testReplicateMessageSynchronous() throws Exception {
        batchingManager.becomeLeaderForPartition(partition, Arrays.asList(1, 2, 3));
        
        // Test synchronous replication (should use batching internally)
        Message message = createTestMessage("key1", "value1");
        
        // This should complete without throwing exceptions
        assertDoesNotThrow(() -> batchingManager.replicateMessage(partition, message));
    }
    
    private Message createTestMessage(String key, String value) {
        return new Message(key, value.getBytes(), "test-topic", 0, 0L, 
                          Instant.now(), new HashMap<>());
    }
}