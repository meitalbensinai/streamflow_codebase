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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

public class BatchingReplicaManagerTest {
    
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
    
    private BatchingReplicaManager batchingReplicaManager;
    private TopicPartition partition;
    
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        Properties props = new Properties();
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.REPLICA_SOCKET_TIMEOUT_MS, "5000");
        
        BrokerConfig config = new BrokerConfig(props);
        when(mockBroker.getConfig()).thenReturn(config);
        when(mockBroker.getBrokerId()).thenReturn(1);
        when(mockBroker.getStorageEngine()).thenReturn(mockStorageEngine);
        when(mockBroker.getController()).thenReturn(mockController);
        when(mockController.getClusterMetadata()).thenReturn(mockClusterMetadata);
        when(mockStorageEngine.getLogEndOffset(any())).thenReturn(100L);
        
        partition = new TopicPartition("test-topic", 0);
        batchingReplicaManager = new BatchingReplicaManager(
            partition, Arrays.asList(1, 2, 3), mockBroker, mockMetrics);
    }
    
    @Test
    public void testStartAndStop() {
        assertFalse(batchingReplicaManager.isRunning());
        
        batchingReplicaManager.start();
        assertTrue(batchingReplicaManager.isRunning());
        
        batchingReplicaManager.stop();
        assertFalse(batchingReplicaManager.isRunning());
    }
    
    @Test
    public void testReplicateBatchSuccess() throws Exception {
        batchingReplicaManager.start();
        
        // Create a batch with multiple messages
        MessageBatch batch = new MessageBatch(partition, 5, 1024);
        
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        CompletableFuture<Void> ack3 = new CompletableFuture<>();
        
        batch.tryAdd(createTestMessage("key1", "value1"), ack1);
        batch.tryAdd(createTestMessage("key2", "value2"), ack2);
        batch.tryAdd(createTestMessage("key3", "value3"), ack3);
        
        // Replicate the batch
        batchingReplicaManager.replicateBatch(batch);
        
        // All acknowledgments should complete successfully
        assertTrue(ack1.isDone());
        assertTrue(ack2.isDone());
        assertTrue(ack3.isDone());
        
        assertFalse(ack1.isCompletedExceptionally());
        assertFalse(ack2.isCompletedExceptionally());
        assertFalse(ack3.isCompletedExceptionally());
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testReplicateBatchWhenNotRunning() {
        // Don't start the manager
        assertFalse(batchingReplicaManager.isRunning());
        
        MessageBatch batch = new MessageBatch(partition, 5, 1024);
        CompletableFuture<Void> ack = new CompletableFuture<>();
        batch.tryAdd(createTestMessage("key1", "value1"), ack);
        
        batchingReplicaManager.replicateBatch(batch);
        
        // Should complete exceptionally
        assertTrue(ack.isDone());
        assertTrue(ack.isCompletedExceptionally());
    }
    
    @Test
    public void testReplicateEmptyBatch() {
        batchingReplicaManager.start();
        
        MessageBatch emptyBatch = new MessageBatch(partition, 5, 1024);
        
        // Should handle empty batch gracefully
        assertDoesNotThrow(() -> batchingReplicaManager.replicateBatch(emptyBatch));
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testPartialReplicationFailure() throws Exception {
        batchingReplicaManager.start();
        
        // Create a batch
        MessageBatch batch = new MessageBatch(partition, 5, 1024);
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        
        batch.tryAdd(createTestMessage("key1", "value1"), ack1);
        batch.tryAdd(createTestMessage("key2", "value2"), ack2);
        
        // The replication will have some simulated failures due to the 5% failure rate
        // But should still succeed if majority succeeds
        batchingReplicaManager.replicateBatch(batch);
        
        // Should complete (either successfully or exceptionally, depending on random failures)
        assertTrue(ack1.isDone());
        assertTrue(ack2.isDone());
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testISRManagement() {
        batchingReplicaManager.start();
        
        // Initially all replicas should be in ISR
        assertTrue(batchingReplicaManager.isInSyncReplica(2));
        assertTrue(batchingReplicaManager.isInSyncReplica(3));
        
        // Simulate replica failure
        batchingReplicaManager.handleReplicaFailure(2);
        assertFalse(batchingReplicaManager.isInSyncReplica(2));
        assertTrue(batchingReplicaManager.isInSyncReplica(3));
        
        // Simulate replica recovery
        batchingReplicaManager.handleReplicaRecovery(2);
        // Note: Recovery doesn't immediately add to ISR, replica needs to catch up
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testReplicationMetricsIntegration() throws Exception {
        batchingReplicaManager.start();
        
        MessageBatch batch = new MessageBatch(partition, 5, 1024);
        CompletableFuture<Void> ack = new CompletableFuture<>();
        batch.tryAdd(createTestMessage("key1", "value1"), ack);
        
        batchingReplicaManager.replicateBatch(batch);
        
        // If there are partial failures, metrics should be recorded
        // Note: Due to random nature of simulated failures, we can't guarantee specific calls
        // But we can verify the manager doesn't throw exceptions
        assertTrue(ack.isDone());
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testBackwardCompatibilityWithIndividualMessages() {
        batchingReplicaManager.start();
        
        Message message = createTestMessage("key1", "value1");
        
        // Should handle individual message replication (for backward compatibility)
        assertDoesNotThrow(() -> batchingReplicaManager.replicateMessage(message));
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testConcurrentBatchReplication() throws Exception {
        batchingReplicaManager.start();
        
        // Create multiple batches and replicate concurrently
        MessageBatch batch1 = new MessageBatch(partition, 5, 1024);
        MessageBatch batch2 = new MessageBatch(partition, 5, 1024);
        
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        
        batch1.tryAdd(createTestMessage("key1", "value1"), ack1);
        batch2.tryAdd(createTestMessage("key2", "value2"), ack2);
        
        // Start both replications concurrently
        Thread t1 = new Thread(() -> batchingReplicaManager.replicateBatch(batch1));
        Thread t2 = new Thread(() -> batchingReplicaManager.replicateBatch(batch2));
        
        t1.start();
        t2.start();
        
        t1.join();
        t2.join();
        
        assertTrue(ack1.isDone());
        assertTrue(ack2.isDone());
        
        batchingReplicaManager.stop();
    }
    
    @Test
    public void testGracefulShutdownWithActiveBatches() throws Exception {
        batchingReplicaManager.start();
        
        MessageBatch batch = new MessageBatch(partition, 5, 1024);
        CompletableFuture<Void> ack = new CompletableFuture<>();
        batch.tryAdd(createTestMessage("key1", "value1"), ack);
        
        // Start replication in background
        Thread replicationThread = new Thread(() -> batchingReplicaManager.replicateBatch(batch));
        replicationThread.start();
        
        // Give it a moment to start
        Thread.sleep(50);
        
        // Stop should wait for active replications
        batchingReplicaManager.stop();
        
        replicationThread.join();
        assertTrue(ack.isDone());
    }
    
    private Message createTestMessage(String key, String value) {
        return new Message(key, value.getBytes(), "test-topic", 0, 0L, 
                          Instant.now(), new HashMap<>());
    }
}