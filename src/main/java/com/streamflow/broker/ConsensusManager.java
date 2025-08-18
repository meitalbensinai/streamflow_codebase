package com.streamflow.broker;

import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.partition.LeaderElection;
import com.streamflow.replication.ReplicationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages distributed consensus for leader election and cluster coordination
 * Provides configuration values for election timeout based on cluster state
 */
public class ConsensusManager {
    private static final Logger logger = LoggerFactory.getLogger(ConsensusManager.class);
    
    private final ClusterMetadata clusterMetadata;
    private final LeaderElection leaderElection;
    private final ReplicationCoordinator replicationCoordinator;
    private final ConfigurationRegistry configRegistry;
    
    private final AtomicLong baseElectionTimeout = new AtomicLong(5000);
    
    public ConsensusManager(ClusterMetadata clusterMetadata, 
                           LeaderElection leaderElection,
                           ReplicationCoordinator replicationCoordinator) {
        this.clusterMetadata = clusterMetadata;
        this.leaderElection = leaderElection;
        this.replicationCoordinator = replicationCoordinator;
        this.configRegistry = ConfigurationRegistry.getInstance();
        
        initializeConsensus();
    }
    
    private void initializeConsensus() {
        // Register the hidden config provider that affects leader election timing
        configRegistry.registerConfigProvider("consensus.leader.election.timeout.ms",
            this::calculateElectionTimeout);
    }
    
    private Long calculateElectionTimeout() {
        // Election timeout depends on cluster size and replication strategy
        int clusterSize = clusterMetadata.getAliveBrokerCount();
        String replicationStrategy = replicationCoordinator.getSyncStrategy();
        
        // Get base timeout from broker configuration instead of hardcoded value
        Object brokerTimeout = configRegistry.getConfigValue("broker.leader.election.timeout");
        long timeout = (brokerTimeout != null) ? ((Number) brokerTimeout).longValue() : baseElectionTimeout.get();
        
        // Adjust timeout based on cluster complexity
        switch (replicationStrategy) {
            case "QUORUM":
                timeout = timeout + (clusterSize * 500); // More time for quorum consensus
                break;
            case "ALL_REPLICAS":
                timeout = timeout + (clusterSize * 1000); // Even more time for all replicas
                break;
            case "LEADER_ONLY":
            default:
                timeout = timeout + (clusterSize * 200); // Minimal adjustment
                break;
        }
        
        return timeout;
    }
    
    public void setBaseElectionTimeout(long timeoutMs) {
        long oldTimeout = baseElectionTimeout.getAndSet(timeoutMs);
        logger.info("Base election timeout changed from {} to {} ms", oldTimeout, timeoutMs);
    }
    
    public long getBaseElectionTimeout() {
        return baseElectionTimeout.get();
    }
    
    public boolean shouldInitiateElection() {
        // Complex logic to determine if election should be initiated
        Integer controllerId = clusterMetadata.getController();
        if (controllerId == null) {
            return true;
        }
        
        // Check if current controller is responsive
        return !clusterMetadata.isBrokerAlive(controllerId);
    }
    
    public void handleElectionTimeout() {
        logger.warn("Election timeout occurred, initiating new election cycle");
        // Trigger new election through leader election component
    }
}