package com.streamflow.partition;

import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Monitors partition health based on configurable strategies
 * Health check mode is dynamically determined by replication coordinator
 */
public class PartitionHealthMonitor {
    private static final Logger logger = LoggerFactory.getLogger(PartitionHealthMonitor.class);
    
    private final ClusterMetadata clusterMetadata;
    private final MetricsCollector metricsCollector;
    private final ConfigurationRegistry configRegistry;
    private final ScheduledExecutorService scheduler;
    
    // This is where the hidden complexity lies - the interval comes from a chain of dependencies
    private volatile long healthCheckIntervalMs = 30000; // Default fallback
    
    public PartitionHealthMonitor(ClusterMetadata clusterMetadata, MetricsCollector metricsCollector) {
        this.clusterMetadata = clusterMetadata;
        this.metricsCollector = metricsCollector;
        this.configRegistry = ConfigurationRegistry.getInstance();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        
        initializeHealthChecking();
    }
    
    private void initializeHealthChecking() {
        // The health check interval is derived from broker config through a complex chain
        // Agent must discover this connection by reading multiple files
        updateHealthCheckInterval();
        
        scheduler.scheduleAtFixedRate(this::performHealthCheck, 
            healthCheckIntervalMs, healthCheckIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    private void updateHealthCheckInterval() {
        // This method holds the key to the puzzle - it connects broker config to health monitoring
        // But the connection is through the configuration registry and replication coordinator
        Object brokerHealthInterval = configRegistry.getConfigValue("broker.partition.health.strategy");
        if (brokerHealthInterval != null) {
            // The interval is derived from the health check mode, not directly configured
            String healthMode = brokerHealthInterval.toString();
            healthCheckIntervalMs = calculateIntervalFromMode(healthMode);
            logger.debug("Updated health check interval to {} ms based on mode: {}", 
                        healthCheckIntervalMs, healthMode);
        }
    }
    
    private long calculateIntervalFromMode(String healthMode) {
        // Different health modes require different check frequencies
        switch (healthMode) {
            case "ISR_BASED":
                return 15000; // More frequent for quorum-based systems
            case "LEADER_HEALTH":
                return 30000; // Standard interval for leader-only
            case "ALL_REPLICA_HEALTH":
                return 10000; // Very frequent for all-replica monitoring
            case "BASIC":
            default:
                return 60000; // Less frequent for basic monitoring
        }
    }
    
    private void performHealthCheck() {
        try {
            String healthMode = getHealthCheckMode();
            List<TopicPartition> unhealthyPartitions = findUnhealthyPartitions(healthMode);
            
            for (TopicPartition partition : unhealthyPartitions) {
                handleUnhealthyPartition(partition);
            }
            
            metricsCollector.recordRequestProcessed("partition.health.check", 
                System.currentTimeMillis() % 1000);
                
        } catch (Exception e) {
            logger.error("Error during partition health check", e);
        }
    }
    
    private String getHealthCheckMode() {
        Object mode = configRegistry.getConfigValue("partition.monitor.health.check.mode");
        return mode != null ? mode.toString() : "BASIC";
    }
    
    private List<TopicPartition> findUnhealthyPartitions(String healthMode) {
        // Implementation would check partitions based on the health mode
        // This is simplified for the challenge
        return List.of();
    }
    
    private void handleUnhealthyPartition(TopicPartition partition) {
        logger.warn("Detected unhealthy partition: {}", partition);
        // Trigger remedial actions
    }
    
    public long getHealthCheckInterval() {
        return healthCheckIntervalMs;
    }
    
    public void shutdown() {
        scheduler.shutdown();
    }
}