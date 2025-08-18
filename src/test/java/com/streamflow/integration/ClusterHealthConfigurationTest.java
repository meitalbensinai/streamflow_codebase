package com.streamflow.integration;

import com.streamflow.broker.BrokerController;
import com.streamflow.broker.BrokerNode;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.broker.PartitionManager;
import com.streamflow.config.BrokerConfig;
import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.partition.PartitionHealthMonitor;
import com.streamflow.replication.ReplicationManager;
import com.streamflow.storage.StorageEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that validates the complex configuration dependency chain
 * from BrokerConfig through ConfigurationRegistry to PartitionHealthMonitor
 * 
 * This test fails because the partition health check interval is not properly
 * connected to the broker configuration through the dependency chain.
 */
public class ClusterHealthConfigurationTest {
    
    private BrokerConfig brokerConfig;
    private ClusterMetadata clusterMetadata;
    private PartitionManager partitionManager;
    private ReplicationManager replicationManager;
    private MetricsCollector metricsCollector;
    private BrokerController brokerController;
    
    @BeforeEach
    public void setUp() {
        // Configure broker with specific settings that should affect health monitoring
        Properties props = new Properties();
        props.setProperty("broker.id", "1");
        props.setProperty("host.name", "localhost");
        props.setProperty("port", "9092");
        
        // These settings should flow through the dependency chain to affect health monitoring
        props.setProperty("offsets.topic.replication.factor", "3");
        props.setProperty("auto.leader.rebalance.enable", "true");
        props.setProperty("controlled.shutdown.enable", "true");
        props.setProperty("replica.socket.timeout.ms", "30000");
        props.setProperty("controlled.shutdown.max.retries", "5");
        props.setProperty("controlled.shutdown.retry.backoff.ms", "2000");
        props.setProperty("log.segment.bytes", "15000000"); // 15MB
        props.setProperty("num.network.threads", "6");
        props.setProperty("auto.create.topics.enable", "true");

        
        brokerConfig = new BrokerConfig(props);
        clusterMetadata = new ClusterMetadata();
        metricsCollector = new MetricsCollector();
        
        // Create mock dependencies for PartitionManager
        StorageEngine storageEngine = new MockStorageEngine();
        replicationManager = new ReplicationManager();
        BrokerNode brokerNode = new BrokerNode(1, "localhost", 9092, brokerConfig, 
                                              clusterMetadata, storageEngine, replicationManager, metricsCollector);
        partitionManager = new PartitionManager(brokerNode, storageEngine, replicationManager);
        
        // Creating broker controller initializes the complex dependency chain
        brokerController = new BrokerController(
            brokerConfig, clusterMetadata, partitionManager, 
            replicationManager, metricsCollector
        );
    }
    
    @Test
    public void testHealthMonitoringConfigurationChain() {
        // This test verifies that broker configuration properly flows through
        // the ConfigurationRegistry to affect PartitionHealthMonitor behavior
        
        ConfigurationRegistry registry = ConfigurationRegistry.getInstance();
        
        // Verify replication sync mode is derived from broker config
        Object syncMode = registry.getConfigValue("broker.replication.sync.mode");
        assertNotNull(syncMode, "Replication sync mode should be configured");
        assertEquals("QUORUM", syncMode.toString(), 
                    "With replication factor 3 and auto-rebalance enabled, should use QUORUM mode");
        
        // Verify this flows through to health check mode
        Object healthMode = registry.getConfigValue("partition.monitor.health.check.mode");
        assertNotNull(healthMode, "Health check mode should be derived from replication mode");
        assertEquals("ISR_BASED", healthMode.toString(), 
                    "QUORUM replication should result in ISR_BASED health monitoring");
        
        // Verify compression threshold is calculated from log segment size
        Object compressionThreshold = registry.getConfigValue("storage.engine.compression.policy.threshold");
        assertNotNull(compressionThreshold, "Compression threshold should be configured");
        assertEquals(15000L, ((Number) compressionThreshold).longValue(), 
                    "Compression threshold should be 1/1000th of log segment size");
        
        // Verify election timeout is calculated from multiple broker config values  
        Object electionTimeout = registry.getConfigValue("consensus.leader.election.timeout.ms");
        assertNotNull(electionTimeout, "Election timeout should be configured");
        // Expected: 30000 + (5 * 2000) = 40000
        assertEquals(40000L, ((Number) electionTimeout).longValue(),
                    "Election timeout should be socket timeout + (retries * backoff)");
        
        // Verify message routing strategy is derived from network threads and auto-create
        Object routingStrategy = registry.getConfigValue("network.message.dispatcher.routing.algorithm");
        assertNotNull(routingStrategy, "Routing strategy should be configured");
        assertEquals("LOAD_BALANCED", routingStrategy.toString(),
                    "With 6 network threads and auto-create enabled, should use LOAD_BALANCED");
        
        // THE CRITICAL TEST - this will fail without the missing configuration
        // The health check interval should be optimized (5 seconds) instead of default ISR_BASED (15 seconds)
        long expectedInterval = 5000L; // OPTIMIZED strategy interval
        long actualInterval = getHealthMonitorInterval();
        
        assertEquals(expectedInterval, actualInterval,
                    "Health monitor interval should be 5000ms for OPTIMIZED strategy. " +
                    "Current interval: " + actualInterval + "ms. " +
                    "This indicates the partition.health.monitoring.strategy configuration is missing or incorrect.");
    }
    
    private long getHealthMonitorInterval() {
        // This is a complex method that requires understanding the internal structure
        // The agent must figure out how to extract the health monitor from broker controller
        // and get its interval, which depends on the configuration chain
        
        // Since fields are private, the agent must use reflection or find another way
        // This adds another layer of complexity to the challenge
        try {
            java.lang.reflect.Field healthMonitorField = BrokerController.class.getDeclaredField("healthMonitor");
            healthMonitorField.setAccessible(true);
            PartitionHealthMonitor healthMonitor = (PartitionHealthMonitor) healthMonitorField.get(brokerController);
            return healthMonitor.getHealthCheckInterval();
        } catch (Exception e) {
            fail("Unable to access health monitor interval: " + e.getMessage());
            return -1;
        }
    }
    
    @Test
    public void testConfigurationMappingChain() {
        // Verify that the configuration mapping chain is working correctly
        ConfigurationRegistry registry = ConfigurationRegistry.getInstance();
        
        // Test direct configuration access
        assertTrue(registry.hasConfigProvider("broker.replication.sync.mode"));
        assertTrue(registry.hasConfigProvider("broker.partition.health.strategy"));
        
        // Test mapped configuration access (this is the hidden complexity)
        assertTrue(registry.hasConfigProvider("replication.coordinator.sync.strategy"));
        assertTrue(registry.hasConfigProvider("partition.monitor.health.check.mode"));
        
        // Verify mappings work correctly
        Object directValue = registry.getConfigValue("broker.replication.sync.mode");
        Object mappedValue = registry.getConfigValue("replication.coordinator.sync.strategy");
        assertEquals(directValue, mappedValue, "Mapped configuration should return same value as direct");
    }
    
    // Mock storage engine for testing
    private static class MockStorageEngine implements StorageEngine {
        @Override
        public void initialize() {}
        
        @Override
        public void shutdown() {}
        
        @Override
        public long append(com.streamflow.core.TopicPartition partition, com.streamflow.core.Message message) {
            return 0;
        }
        
        @Override
        public java.util.List<com.streamflow.core.Message> read(com.streamflow.core.TopicPartition partition, long startOffset, int maxMessages) {
            return java.util.List.of();
        }
        
        @Override
        public long getLogEndOffset(com.streamflow.core.TopicPartition partition) {
            return 0;
        }
        
        @Override
        public long getLogStartOffset(com.streamflow.core.TopicPartition partition) {
            return 0;
        }
        
        @Override
        public void createPartition(com.streamflow.core.TopicPartition partition) {}
        
        @Override
        public void deletePartition(com.streamflow.core.TopicPartition partition) {}
        
        @Override
        public void flush(com.streamflow.core.TopicPartition partition) {}
        
        @Override
        public void truncate(com.streamflow.core.TopicPartition partition, long offset) {}
        
        @Override
        public com.streamflow.storage.StorageMetrics getMetrics(com.streamflow.core.TopicPartition partition) {
            return null;
        }
    }
}