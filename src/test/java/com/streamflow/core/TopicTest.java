package com.streamflow.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TopicTest {
    
    private Topic topic;
    private String topicName;
    private int partitionCount;
    private int replicationFactor;
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        topicName = "test-topic";
        partitionCount = 3;
        replicationFactor = 2;
        config = new HashMap<>();
        config.put("retention.ms", "86400000");
        config.put("max.message.bytes", "2097152");
        
        topic = new Topic(topicName, partitionCount, replicationFactor, config);
    }

    @Test
    void testTopicCreation() {
        assertNotNull(topic);
        assertEquals(topicName, topic.getName());
        assertEquals(partitionCount, topic.getPartitionCount());
        assertEquals(replicationFactor, topic.getReplicationFactor());
        assertEquals(config, topic.getConfig());
    }

    @Test
    void testPartitionCreation() {
        List<TopicPartition> partitions = topic.getAllPartitions();
        assertEquals(partitionCount, partitions.size());
        
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition partition = topic.getPartition(i);
            assertNotNull(partition);
            assertEquals(topicName, partition.getTopic());
            assertEquals(i, partition.getPartition());
        }
    }

    @Test
    void testRetentionConfiguration() {
        assertEquals(86400000L, topic.getRetentionMs());
        
        // Test default retention
        Topic defaultTopic = new Topic("default-topic", 1, 1, new HashMap<>());
        assertEquals(604800000L, defaultTopic.getRetentionMs()); // 7 days default
    }

    @Test
    void testMaxMessageSizeConfiguration() {
        assertEquals(2097152, topic.getMaxMessageSize());
        
        // Test default max message size
        Topic defaultTopic = new Topic("default-topic", 1, 1, new HashMap<>());
        assertEquals(1048576, defaultTopic.getMaxMessageSize()); // 1MB default
    }

    @Test
    void testInvalidPartitionAccess() {
        assertNull(topic.getPartition(-1));
        assertNull(topic.getPartition(partitionCount));
        assertNull(topic.getPartition(100));
    }

    @Test
    void testTopicToString() {
        String topicString = topic.toString();
        assertTrue(topicString.contains(topicName));
        assertTrue(topicString.contains(String.valueOf(partitionCount)));
        assertTrue(topicString.contains(String.valueOf(replicationFactor)));
    }

    @Test
    void testPartitionListImmutability() {
        List<TopicPartition> partitions1 = topic.getAllPartitions();
        List<TopicPartition> partitions2 = topic.getAllPartitions();
        
        // Should be different instances
        assertNotSame(partitions1, partitions2);
        // But with same content
        assertEquals(partitions1, partitions2);
    }
}