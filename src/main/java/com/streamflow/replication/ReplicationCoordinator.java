package com.streamflow.replication;

import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.broker.ClusterMetadata;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates replication strategy across the cluster
 * Manages sync vs async replication modes based on configuration
 */
public class ReplicationCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationCoordinator.class);
    
    private final ClusterMetadata clusterMetadata;
    private final ConfigurationRegistry configRegistry;
    private final AtomicReference<String> syncStrategy = new AtomicReference<>("QUORUM");
    
    public ReplicationCoordinator(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
        this.configRegistry = ConfigurationRegistry.getInstance();
        initializeStrategy();
    }
    
    private void initializeStrategy() {
        // Register config provider for replication strategy
        configRegistry.registerConfigProvider("replication.coordinator.sync.strategy", 
            () -> syncStrategy.get());
            
        // This is the hidden connection - partition health depends on replication strategy
        configRegistry.registerConfigProvider("partition.monitor.health.check.mode",
            () -> deriveHealthCheckMode());
    }
    
    private String deriveHealthCheckMode() {
        String strategy = syncStrategy.get();
        switch (strategy) {
            case "QUORUM":
                return "ISR_BASED";
            case "LEADER_ONLY":
                return "LEADER_HEALTH";
            case "ALL_REPLICAS":
                return "ALL_REPLICA_HEALTH";
            default:
                return "BASIC";
        }
    }
    
    public void setSyncStrategy(String strategy) {
        String oldStrategy = syncStrategy.getAndSet(strategy);
        logger.info("Replication sync strategy changed from {} to {}", oldStrategy, strategy);
    }
    
    public String getSyncStrategy() {
        return syncStrategy.get();
    }
    
    public boolean shouldWaitForQuorum(TopicPartition partition) {
        return "QUORUM".equals(syncStrategy.get()) && 
               clusterMetadata.getInSyncReplicas(partition).size() > 1;
    }
    
    public int getRequiredReplicas(TopicPartition partition) {
        switch (syncStrategy.get()) {
            case "QUORUM":
                return (clusterMetadata.getReplicas(partition).size() / 2) + 1;
            case "ALL_REPLICAS":
                return clusterMetadata.getReplicas(partition).size();
            case "LEADER_ONLY":
            default:
                return 1;
        }
    }
}