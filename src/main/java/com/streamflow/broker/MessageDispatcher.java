package com.streamflow.broker;

import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Dispatches messages across the network to appropriate brokers
 * Routing algorithm affects how messages are processed and can impact health monitoring
 */
public class MessageDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(MessageDispatcher.class);
    
    private final ClusterMetadata clusterMetadata;
    private final ConfigurationRegistry configRegistry;
    private final AtomicReference<String> routingAlgorithm = new AtomicReference<>("ROUND_ROBIN");
    
    public MessageDispatcher(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
        this.configRegistry = ConfigurationRegistry.getInstance();
        initializeDispatcher();
    }
    
    private void initializeDispatcher() {
        // Register the routing algorithm provider
        configRegistry.registerConfigProvider("network.message.dispatcher.routing.algorithm",
            () -> routingAlgorithm.get());
    }
    
    public void setRoutingAlgorithm(String algorithm) {
        String oldAlgorithm = routingAlgorithm.getAndSet(algorithm);
        logger.info("Message routing algorithm changed from {} to {}", oldAlgorithm, algorithm);
    }
    
    public String getRoutingAlgorithm() {
        return routingAlgorithm.get();
    }
    
    public BrokerNode selectTargetBroker(Message message) {
        TopicPartition partition = new TopicPartition(message.getTopic(), message.getPartition());
        
        switch (routingAlgorithm.get()) {
            case "CONSISTENT_HASH":
                return selectByHash(message);
            case "LEADER_ONLY":
                return selectLeader(partition);
            case "LOAD_BALANCED":
                return selectLeastLoaded();
            case "ROUND_ROBIN":
            default:
                return selectRoundRobin(partition);
        }
    }
    
    private BrokerNode selectByHash(Message message) {
        // Hash-based selection
        int hash = message.getKey() != null ? message.getKey().hashCode() : 0;
        var brokers = clusterMetadata.getAliveBrokers();
        if (brokers.isEmpty()) return null;
        return brokers.get(Math.abs(hash) % brokers.size());
    }
    
    private BrokerNode selectLeader(TopicPartition partition) {
        var metadata = clusterMetadata.getPartitionMetadata(partition);
        if (metadata != null) {
            return clusterMetadata.getBroker(metadata.getLeader());
        }
        return null;
    }
    
    private BrokerNode selectLeastLoaded() {
        // Simplified: just return first broker
        var brokers = clusterMetadata.getAliveBrokers();
        return brokers.isEmpty() ? null : brokers.get(0);
    }
    
    private BrokerNode selectRoundRobin(TopicPartition partition) {
        var replicas = clusterMetadata.getReplicas(partition);
        if (replicas.isEmpty()) return null;
        
        // Simple round-robin among replicas
        int index = (int) (System.currentTimeMillis() % replicas.size());
        return clusterMetadata.getBroker(replicas.get(index));
    }
}