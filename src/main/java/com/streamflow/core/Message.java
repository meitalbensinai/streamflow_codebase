package com.streamflow.core;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Message {
    private final String key;
    private final byte[] value;
    private final String topic;
    private final int partition;
    private final long offset;
    private final Instant timestamp;
    private final Map<String, String> headers;
    private final CompressionType compressionType;

    public Message(String key, byte[] value, String topic, int partition, long offset, 
                   Instant timestamp, Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = new HashMap<>(headers);
        this.compressionType = CompressionType.NONE;
    }

    // Simplified constructor for testing
    public Message(byte[] key, byte[] value, Instant timestamp) {
        this(key != null ? new String(key) : null, value, "test-topic", 0, 0L, timestamp, new HashMap<>());
    }

    // Constructor with compression type
    public Message(String key, byte[] value, String topic, int partition, long offset, 
                   Instant timestamp, Map<String, String> headers, CompressionType compressionType) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = new HashMap<>(headers);
        this.compressionType = compressionType;
    }

    public String getKey() { return key; }
    public byte[] getValue() { return value; }
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public Instant getTimestamp() { return timestamp; }
    public Map<String, String> getHeaders() { return new HashMap<>(headers); }
    public CompressionType getCompressionType() { return compressionType; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return partition == message.partition &&
               offset == message.offset &&
               Objects.equals(key, message.key) &&
               Objects.equals(topic, message.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, topic, partition, offset);
    }

    public static Message createCompressed(byte[] key, byte[] value, Instant timestamp, CompressionType type) {
        // Factory method for creating compressed messages
        // Must integrate with storage and transmission layers
        // Should handle compression threshold logic based on BrokerConfig
        throw new UnsupportedOperationException("Message compression not implemented");
    }

    public byte[] getCompressedValue() {
        // Return compressed representation if available, otherwise original
        // Must be transparent to existing code that expects raw bytes
        throw new UnsupportedOperationException("Message compression not implemented");
    }

    public int getSizeInBytes() {
        int size = 0;
        if (key != null) {
            size += key.getBytes().length;
        }
        if (value != null) {
            size += value.length;
        }
        return size;
    }

    @Override
    public String toString() {
        return "Message{" +
               "key='" + key + '\'' +
               ", topic='" + topic + '\'' +
               ", partition=" + partition +
               ", offset=" + offset +
               ", timestamp=" + timestamp +
               ", compressionType=" + compressionType +
               '}';
    }
}