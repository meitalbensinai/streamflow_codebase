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

    public Message(String key, byte[] value, String topic, int partition, long offset, 
                   Instant timestamp, Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = new HashMap<>(headers);
    }

    public String getKey() { return key; }
    public byte[] getValue() { return value; }
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public Instant getTimestamp() { return timestamp; }
    public Map<String, String> getHeaders() { return new HashMap<>(headers); }

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

    @Override
    public String toString() {
        return "Message{" +
               "key='" + key + '\'' +
               ", topic='" + topic + '\'' +
               ", partition=" + partition +
               ", offset=" + offset +
               ", timestamp=" + timestamp +
               '}';
    }
}