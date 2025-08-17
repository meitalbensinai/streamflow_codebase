package com.streamflow.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

class MessageTest {
    
    private Message message;
    private String key;
    private byte[] value;
    private String topic;
    private int partition;
    private long offset;
    private Instant timestamp;
    private Map<String, String> headers;

    @BeforeEach
    void setUp() {
        key = "test-key";
        value = "test-value".getBytes();
        topic = "test-topic";
        partition = 0;
        offset = 100L;
        timestamp = Instant.now();
        headers = new HashMap<>();
        headers.put("header1", "value1");
        headers.put("header2", "value2");
        
        message = new Message(key, value, topic, partition, offset, timestamp, headers);
    }

    @Test
    void testMessageCreation() {
        assertNotNull(message);
        assertEquals(key, message.getKey());
        assertArrayEquals(value, message.getValue());
        assertEquals(topic, message.getTopic());
        assertEquals(partition, message.getPartition());
        assertEquals(offset, message.getOffset());
        assertEquals(timestamp, message.getTimestamp());
        assertEquals(headers, message.getHeaders());
    }

    @Test
    void testMessageEquality() {
        Message sameMessage = new Message(key, value, topic, partition, offset, timestamp, headers);
        Message differentMessage = new Message(key, value, topic, partition, 101L, timestamp, headers);
        
        assertEquals(message, sameMessage);
        assertNotEquals(message, differentMessage);
    }

    @Test
    void testMessageHashCode() {
        Message sameMessage = new Message(key, value, topic, partition, offset, timestamp, headers);
        assertEquals(message.hashCode(), sameMessage.hashCode());
    }

    @Test
    void testMessageToString() {
        String messageString = message.toString();
        assertTrue(messageString.contains(key));
        assertTrue(messageString.contains(topic));
        assertTrue(messageString.contains(String.valueOf(partition)));
        assertTrue(messageString.contains(String.valueOf(offset)));
    }

    @Test
    void testMessageWithNullKey() {
        Message nullKeyMessage = new Message(null, value, topic, partition, offset, timestamp, headers);
        assertNull(nullKeyMessage.getKey());
        assertEquals(topic, nullKeyMessage.getTopic());
    }

    @Test
    void testMessageImmutability() {
        // Verify that modifying the headers map doesn't affect the message
        Map<String, String> originalHeaders = new HashMap<>(headers);
        headers.put("new-header", "new-value");
        
        assertEquals(originalHeaders.size(), message.getHeaders().size());
        assertFalse(message.getHeaders().containsKey("new-header"));
    }

    @Test
    void testMessageCompression() {
        // Test data that should benefit from compression
        String repetitiveData = "This is a test message that contains repetitive data. ".repeat(50);
        byte[] largeValue = repetitiveData.getBytes();
        byte[] key = "compression-test-key".getBytes();
        
        // Test compression factory method - should throw UnsupportedOperationException until implemented
        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> {
            Message.createCompressed(key, largeValue, Instant.now(), CompressionType.GZIP);
        });
        
        assertEquals("Message compression not implemented", exception.getMessage());
        
        // TODO: When implemented, this test should verify:
        // - Compressed message is created with correct compression type
        // - Compressed data is smaller than original
        // - Original data can be recovered from compressed message
    }
    
    @Test
    void testCompressionThreshold() {
        // Small messages compression threshold test - should throw UnsupportedOperationException until implemented
        byte[] smallValue = "small".getBytes();
        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> {
            Message.createCompressed("key".getBytes(), smallValue, Instant.now(), CompressionType.GZIP);
        });
        
        assertEquals("Message compression not implemented", exception.getMessage());
        
        // TODO: When implemented, this test should verify:
        // - Small messages fall back to no compression (CompressionType.NONE)
    }
}