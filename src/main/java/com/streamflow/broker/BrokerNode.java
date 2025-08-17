package com.streamflow.broker;

import com.streamflow.config.BrokerConfig;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.replication.ReplicationManager;
import com.streamflow.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class BrokerNode {
    private static final Logger logger = LoggerFactory.getLogger(BrokerNode.class);
    
    private final int brokerId;
    private final String host;
    private final int port;
    private final BrokerConfig config;
    private final ClusterMetadata clusterMetadata;
    private final StorageEngine storageEngine;
    private final ReplicationManager replicationManager;
    private final MetricsCollector metricsCollector;
    private final BrokerController controller;
    private final PartitionManager partitionManager;
    private final RequestProcessor requestProcessor;
    
    private final Set<TopicPartition> leaderPartitions;
    private final Set<TopicPartition> followerPartitions;
    private final AtomicBoolean isRunning;

    public BrokerNode(int brokerId, String host, int port, BrokerConfig config,
                      ClusterMetadata clusterMetadata, StorageEngine storageEngine, 
                      ReplicationManager replicationManager, MetricsCollector metricsCollector) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.config = config;
        this.clusterMetadata = clusterMetadata;
        this.storageEngine = storageEngine;
        this.replicationManager = replicationManager;
        this.metricsCollector = metricsCollector;
        this.partitionManager = new PartitionManager(this, storageEngine, replicationManager);
        this.controller = new BrokerController(config, clusterMetadata, partitionManager, replicationManager, metricsCollector);
        this.requestProcessor = new RequestProcessor(this, partitionManager, metricsCollector);
        
        this.leaderPartitions = ConcurrentHashMap.newKeySet();
        this.followerPartitions = ConcurrentHashMap.newKeySet();
        this.isRunning = new AtomicBoolean(false);
    }

    public void startup() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting broker {} on {}:{}", brokerId, host, port);
            
            storageEngine.initialize();
            replicationManager.initialize(this);
            controller.startup();
            partitionManager.startup();
            requestProcessor.startup();
            
            metricsCollector.recordBrokerStartup(brokerId);
            logger.info("Broker {} started successfully", brokerId);
        }
    }

    public void shutdown() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Shutting down broker {}", brokerId);
            
            requestProcessor.shutdown();
            partitionManager.shutdown();
            controller.shutdown();
            replicationManager.shutdown();
            storageEngine.shutdown();
            
            metricsCollector.recordBrokerShutdown(brokerId);
            logger.info("Broker {} shut down successfully", brokerId);
        }
    }

    public void becomeLeaderFor(TopicPartition partition) {
        leaderPartitions.add(partition);
        followerPartitions.remove(partition);
        partitionManager.promoteToLeader(partition);
        logger.info("Broker {} became leader for partition {}", brokerId, partition);
    }

    public void becomeFollowerFor(TopicPartition partition) {
        followerPartitions.add(partition);
        leaderPartitions.remove(partition);
        partitionManager.demoteToFollower(partition);
        logger.info("Broker {} became follower for partition {}", brokerId, partition);
    }

    public boolean isLeaderFor(TopicPartition partition) {
        return leaderPartitions.contains(partition);
    }

    public boolean isFollowerFor(TopicPartition partition) {
        return followerPartitions.contains(partition);
    }

    // Getters
    public int getBrokerId() { return brokerId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public BrokerConfig getConfig() { return config; }
    public StorageEngine getStorageEngine() { return storageEngine; }
    public ReplicationManager getReplicationManager() { return replicationManager; }
    public MetricsCollector getMetricsCollector() { return metricsCollector; }
    public BrokerController getController() { return controller; }
    public PartitionManager getPartitionManager() { return partitionManager; }
    public RequestProcessor getRequestProcessor() { return requestProcessor; }
    public Set<TopicPartition> getLeaderPartitions() { return Set.copyOf(leaderPartitions); }
    public Set<TopicPartition> getFollowerPartitions() { return Set.copyOf(followerPartitions); }
    public boolean isRunning() { return isRunning.get(); }

    @Override
    public String toString() {
        return "BrokerNode{" +
               "brokerId=" + brokerId +
               ", host='" + host + '\'' +
               ", port=" + port +
               ", isRunning=" + isRunning.get() +
               '}';
    }
}