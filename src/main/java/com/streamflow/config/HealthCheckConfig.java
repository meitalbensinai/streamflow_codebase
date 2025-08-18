package com.streamflow.config;

/**
 * Configuration specifically for health checking intervals
 * 
 * NOTE: This is a DECOY class - it appears to be related to health check configuration
 * but is NOT used by the actual health monitoring system. The real configuration
 * comes through the ConfigurationRegistry dependency chain from BrokerConfig.
 * 
 * This class exists to mislead agents into thinking this is where they need to make changes.
 */
public class HealthCheckConfig {
    
    // These constants appear relevant but are NOT used by the real system
    public static final String PARTITION_HEALTH_CHECK_INTERVAL = "partition.health.check.interval.ms";
    public static final String REPLICATION_HEALTH_CHECK_INTERVAL = "replication.health.check.interval.ms";
    public static final String CLUSTER_HEALTH_CHECK_INTERVAL = "cluster.health.check.interval.ms";
    
    // Default intervals that look like they should be used but aren't
    public static final long DEFAULT_PARTITION_HEALTH_INTERVAL = 30000L;
    public static final long DEFAULT_REPLICATION_HEALTH_INTERVAL = 15000L;
    public static final long DEFAULT_CLUSTER_HEALTH_INTERVAL = 60000L;
    
    private HealthCheckConfig() {
        // Static utility class
    }
    
    public static long getPartitionHealthCheckInterval() {
        return DEFAULT_PARTITION_HEALTH_INTERVAL;
    }
    
    public static long getReplicationHealthCheckInterval() {
        return DEFAULT_REPLICATION_HEALTH_INTERVAL;
    }
    
    public static long getClusterHealthCheckInterval() {
        return DEFAULT_CLUSTER_HEALTH_INTERVAL;
    }
    
    // Method that agents might think they need to modify
    public static long calculateOptimizedInterval(String strategy) {
        switch (strategy.toUpperCase()) {
            case "AGGRESSIVE":
                return 5000L;
            case "OPTIMIZED":
                return 10000L;
            case "CONSERVATIVE":
                return 30000L;
            default:
                return DEFAULT_PARTITION_HEALTH_INTERVAL;
        }
    }
}