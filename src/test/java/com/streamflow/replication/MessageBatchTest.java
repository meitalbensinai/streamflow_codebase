package com.streamflow.replication;

import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class MessageBatchTest {
    
    private TopicPartition partition;
    private MessageBatch batch;
    
    @BeforeEach
    public void setUp() {
        partition = new TopicPartition("test-topic", 0);
        batch = new MessageBatch(partition, 10, 1024);
    }
    
    @Test
    public void testBatchCreation() {
        assertEquals(partition, batch.getPartition());
        assertTrue(batch.isEmpty());
        assertEquals(0, batch.size());
        assertEquals(0, batch.getTotalBytes());
        assertFalse(batch.isSealed());
    }
    
    @Test
    public void testAddMessage() {
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        
        assertTrue(batch.tryAdd(message, ackFuture));
        
        assertFalse(batch.isEmpty());
        assertEquals(1, batch.size());
        assertTrue(batch.getTotalBytes() > 0);
        assertEquals(1, batch.getMessages().size());
    }
    
    @Test
    public void testBatchSizeLimits() {
        // Fill batch to size limit
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("key" + i, "value" + i);
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            assertTrue(batch.tryAdd(message, ackFuture));
        }
        
        // Next message should fail
        Message extraMessage = createTestMessage("extra", "extra");
        CompletableFuture<Void> extraAck = new CompletableFuture<>();
        assertFalse(batch.tryAdd(extraMessage, extraAck));
        
        assertEquals(10, batch.size());
    }
    
    @Test
    public void testBatchByteLimits() {
        MessageBatch smallBatch = new MessageBatch(partition, 100, 50); // 50 byte limit
        
        // Create a large message
        byte[] largeValue = new byte[60];
        Message largeMessage = new Message("key", largeValue, "test-topic", 0, 0L, 
                                         Instant.now(), new HashMap<>());
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        
        assertFalse(smallBatch.tryAdd(largeMessage, ackFuture));
        assertTrue(smallBatch.isEmpty());
    }
    
    @Test
    public void testBatchSealing() {
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        
        assertTrue(batch.tryAdd(message, ackFuture));
        batch.seal();
        
        assertTrue(batch.isSealed());
        
        // Should not be able to add after sealing
        Message extraMessage = createTestMessage("key2", "value2");
        CompletableFuture<Void> extraAck = new CompletableFuture<>();
        assertFalse(batch.tryAdd(extraMessage, extraAck));
    }
    
    @Test
    public void testShouldFlushTimeout() {
        Message message = createTestMessage("key1", "value1");
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        batch.tryAdd(message, ackFuture);
        
        // Should not flush immediately
        assertFalse(batch.shouldFlush(1000));
        
        // Wait a bit and then should flush on timeout
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        assertTrue(batch.shouldFlush(10)); // 10ms timeout
    }
    
    @Test
    public void testShouldFlushSize() {
        // Fill batch to size limit
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("key" + i, "value" + i);
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            batch.tryAdd(message, ackFuture);
        }
        
        assertTrue(batch.shouldFlush(10000)); // Should flush due to size
    }
    
    @Test
    public void testCompleteAll() {
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        
        batch.tryAdd(createTestMessage("key1", "value1"), ack1);
        batch.tryAdd(createTestMessage("key2", "value2"), ack2);
        
        batch.completeAll();
        
        assertTrue(ack1.isDone());
        assertTrue(ack2.isDone());
        assertFalse(ack1.isCompletedExceptionally());
        assertFalse(ack2.isCompletedExceptionally());
    }
    
    @Test
    public void testCompleteAllExceptionally() {
        CompletableFuture<Void> ack1 = new CompletableFuture<>();
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        
        batch.tryAdd(createTestMessage("key1", "value1"), ack1);
        batch.tryAdd(createTestMessage("key2", "value2"), ack2);
        
        RuntimeException error = new RuntimeException("Test error");
        batch.completeAllExceptionally(error);
        
        assertTrue(ack1.isDone());
        assertTrue(ack2.isDone());
        assertTrue(ack1.isCompletedExceptionally());
        assertTrue(ack2.isCompletedExceptionally());
    }
    
    @Test
    public void testEfficiencyRatio() {
        // Empty batch should have 0 efficiency
        assertEquals(0.0, batch.getEfficiencyRatio());
        
        // Add 5 messages to a batch with max size 10
        for (int i = 0; i < 5; i++) {
            Message message = createTestMessage("key" + i, "value" + i);
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            batch.tryAdd(message, ackFuture);
        }
        
        assertEquals(0.5, batch.getEfficiencyRatio(), 0.01);
    }
    
    @Test
    public void testWrongPartition() {
        TopicPartition wrongPartition = new TopicPartition("wrong-topic", 1);
        Message wrongMessage = new Message("key", "value".getBytes(), "wrong-topic", 1, 0L,
                                         Instant.now(), new HashMap<>());
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        
        assertFalse(batch.tryAdd(wrongMessage, ackFuture));
        assertTrue(batch.isEmpty());
    }
    
    private Message createTestMessage(String key, String value) {
        return new Message(key, value.getBytes(), "test-topic", 0, 0L, 
                          Instant.now(), new HashMap<>());
    }
}