package com.streamflow.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String name;
    private final int partitionCount;
    private final int replicationFactor;
    private final Map<String, String> config;
    private final Map<Integer, TopicPartition> partitions;

    public Topic(String name, int partitionCount, int replicationFactor, Map<String, String> config) {
        this.name = name;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
        this.config = config;
        this.partitions = new ConcurrentHashMap<>();
        
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(i, new TopicPartition(name, i));
        }
    }

    public String getName() { return name; }
    public int getPartitionCount() { return partitionCount; }
    public int getReplicationFactor() { return replicationFactor; }
    public Map<String, String> getConfig() { return config; }
    
    public TopicPartition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }
    
    public List<TopicPartition> getAllPartitions() {
        return List.copyOf(partitions.values());
    }

    public long getRetentionMs() {
        return Long.parseLong(config.getOrDefault("retention.ms", "604800000")); // 7 days default
    }

    public int getMaxMessageSize() {
        return Integer.parseInt(config.getOrDefault("max.message.bytes", "1048576")); // 1MB default
    }

    @Override
    public String toString() {
        return "Topic{" +
               "name='" + name + '\'' +
               ", partitionCount=" + partitionCount +
               ", replicationFactor=" + replicationFactor +
               '}';
    }
}