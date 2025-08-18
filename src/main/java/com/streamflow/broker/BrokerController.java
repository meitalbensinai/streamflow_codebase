package com.streamflow.broker;

import com.streamflow.config.BrokerConfig;
import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;
import com.streamflow.partition.PartitionRebalancer;
import com.streamflow.partition.PartitionHealthMonitor;
import com.streamflow.partition.PartitionHealthConfiguration;
import com.streamflow.partition.LeaderElection;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.replication.ReplicationManager;
import com.streamflow.replication.ReplicationCoordinator;
import com.streamflow.storage.StorageCompressionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerController {
    private static final Logger logger = LoggerFactory.getLogger(BrokerController.class);
    
    private final BrokerConfig brokerConfig;
    private final ClusterMetadata clusterMetadata;
    private final PartitionManager partitionManager;
    private final ReplicationManager replicationManager;
    private final MetricsCollector metricsCollector;
    private final PartitionRebalancer partitionRebalancer;
    private final LeaderElection leaderElection;
    private final ReplicationCoordinator replicationCoordinator;
    private final ConsensusManager consensusManager;
    private final PartitionHealthMonitor healthMonitor;
    private final MessageDispatcher messageDispatcher;
    private final StorageCompressionPolicy storageCompressionPolicy;
    private final ConfigurationRegistry configRegistry;
    private final ScheduledExecutorService scheduledExecutor;
    
    private final Map<String, Topic> topics;
    private volatile boolean isController;

    public BrokerController(BrokerConfig brokerConfig, 
                          ClusterMetadata clusterMetadata, 
                          PartitionManager partitionManager,
                          ReplicationManager replicationManager,
                          MetricsCollector metricsCollector) {
        this.brokerConfig = brokerConfig;
        this.clusterMetadata = clusterMetadata;
        this.partitionManager = partitionManager;
        this.replicationManager = replicationManager;
        this.metricsCollector = metricsCollector;
        this.configRegistry = ConfigurationRegistry.getInstance();
        
        // Initialize components in dependency order - this creates the complex chain
        this.replicationCoordinator = new ReplicationCoordinator(clusterMetadata);
        this.leaderElection = new LeaderElection(this, clusterMetadata);
        this.consensusManager = new ConsensusManager(clusterMetadata, leaderElection, replicationCoordinator);
        this.healthMonitor = new PartitionHealthMonitor(clusterMetadata, metricsCollector);
        this.messageDispatcher = new MessageDispatcher(clusterMetadata);
        this.storageCompressionPolicy = new StorageCompressionPolicy();
        this.partitionRebalancer = new PartitionRebalancer(this, clusterMetadata);
        
        this.scheduledExecutor = Executors.newScheduledThreadPool(3);
        this.topics = new ConcurrentHashMap<>();
        this.isController = false;
        
        initializeBrokerConfigurationProviders();
        initializeComponentConfiguration();
    }
    
    private void initializeBrokerConfigurationProviders() {
        // THIS IS THE KEY HIDDEN DEPENDENCY CHAIN
        // BrokerConfig values are connected to the complex component network through configuration registry
        
        // Register broker config providers that feed into the dependency chain
        configRegistry.registerConfigProvider("broker.replication.sync.mode", 
            () -> determineSyncMode());
            
        configRegistry.registerConfigProvider("broker.partition.health.strategy",
            () -> determineHealthStrategy());
            
        configRegistry.registerConfigProvider("broker.storage.compression.threshold",
            () -> getCompressionThreshold());
            
        configRegistry.registerConfigProvider("broker.leader.election.timeout",
            () -> getElectionTimeout());
            
        configRegistry.registerConfigProvider("broker.message.routing.strategy",
            () -> getRoutingStrategy());
    }
    
    private void initializeComponentConfiguration() {
        // Initialize component configurations based on broker config
        messageDispatcher.setRoutingAlgorithm(getRoutingStrategy());
        consensusManager.setBaseElectionTimeout(getElectionTimeout());
        storageCompressionPolicy.setCompressionThreshold(getCompressionThreshold());
    }
    
    private String determineSyncMode() {
        // Complex logic based on broker configuration
        int replicationFactor = brokerConfig.getInt("offsets.topic.replication.factor", 3);
        boolean autoLeaderRebalance = brokerConfig.isAutoLeaderRebalanceEnabled();
        
        if (replicationFactor >= 3 && autoLeaderRebalance) {
            return "QUORUM";
        } else if (replicationFactor == 1) {
            return "LEADER_ONLY";
        } else {
            return "ALL_REPLICAS";
        }
    }
    
    private String determineHealthStrategy() {
        // This connects broker config to partition health monitoring
        Object strategy = brokerConfig.get("partition.health.monitoring.strategy");
        if (strategy != null) {
            return strategy.toString();
        }
        
        // Fallback based on replication configuration to determine optimal mode
        int replicationFactor = brokerConfig.getInt("offsets.topic.replication.factor", 3);
        boolean autoLeaderRebalance = brokerConfig.isAutoLeaderRebalanceEnabled();
        return PartitionHealthConfiguration.determineOptimalMode(replicationFactor, autoLeaderRebalance);
    }
    
    private Long getCompressionThreshold() {
        // Hidden connection to storage compression
        return brokerConfig.getLong("log.segment.bytes") / 1000; // 1/1000th of segment size
    }
    
    private Long getElectionTimeout() {
        // Complex calculation based on multiple broker config values
        long baseTimeout = brokerConfig.getLong("replica.socket.timeout.ms");
        int retries = brokerConfig.getInt("controlled.shutdown.max.retries", 3);
        long backoff = brokerConfig.getLong("controlled.shutdown.retry.backoff.ms");
        
        return baseTimeout + (retries * backoff);
    }
    
    private String getRoutingStrategy() {
        // Determines message routing based on broker configuration
        boolean autoCreateTopics = brokerConfig.isAutoCreateTopicsEnabled();
        int numNetworkThreads = brokerConfig.getNumNetworkThreads();
        
        if (autoCreateTopics && numNetworkThreads > 4) {
            return "LOAD_BALANCED";
        } else if (numNetworkThreads <= 2) {
            return "LEADER_ONLY";
        } else {
            return "ROUND_ROBIN";
        }
    }

    public void startup() {
        logger.info("Starting broker controller");
        
        // Startup will be handled by individual components
        
        // Start periodic tasks
        scheduledExecutor.scheduleAtFixedRate(
            this::performLeaderElection, 
            0, 
            brokerConfig.getLeaderElectionIntervalMs(), 
            TimeUnit.MILLISECONDS
        );
        
        // The health check interval is now determined by the complex dependency chain
        long healthCheckInterval = healthMonitor.getHealthCheckInterval();
        scheduledExecutor.scheduleAtFixedRate(
            this::checkPartitionHealth, 
            healthCheckInterval, 
            healthCheckInterval, 
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Broker controller started for broker {}", brokerConfig.getBrokerId());
    }

    public void shutdown() {
        logger.info("Shutting down broker controller for broker {}", brokerConfig.getBrokerId());
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Broker controller shut down for broker {}", brokerConfig.getBrokerId());
    }

    public void createTopic(String topicName, int partitions, int replicationFactor, 
                           Map<String, String> config) {
        if (topics.containsKey(topicName)) {
            throw new IllegalArgumentException("Topic " + topicName + " already exists");
        }
        
        Topic topic = new Topic(topicName, partitions, replicationFactor, config);
        topics.put(topicName, topic);
        
        // Assign partitions to brokers
        partitionRebalancer.assignPartitionsForNewTopic(topic);
        
        logger.info("Created topic {} with {} partitions and replication factor {}", 
                   topicName, partitions, replicationFactor);
    }

    public void deleteTopic(String topicName) {
        Topic topic = topics.remove(topicName);
        if (topic != null) {
            partitionRebalancer.removePartitionsForTopic(topic);
            logger.info("Deleted topic {}", topicName);
        }
    }

    private void performLeaderElection() {
        try {
            if (leaderElection.shouldBecomeController()) {
                becomeController();
            } else if (isController && !leaderElection.isControllerAlive()) {
                resignController();
            }
        } catch (Exception e) {
            logger.error("Error during leader election", e);
        }
    }

    private void checkPartitionHealth() {
        if (isController) {
            try {
                List<TopicPartition> unhealthyPartitions = findUnhealthyPartitions();
                for (TopicPartition partition : unhealthyPartitions) {
                    leaderElection.electNewLeaderForPartition(partition);
                }
            } catch (Exception e) {
                logger.error("Error during partition health check", e);
            }
        }
    }

    private List<TopicPartition> findUnhealthyPartitions() {
        // Complex logic to identify partitions that need leader re-election
        // This would involve checking ISR, replica lag, etc.
        return List.of(); // Simplified for now
    }

    private void becomeController() {
        if (!isController) {
            isController = true;
            logger.info("Broker {} became the cluster controller", brokerConfig.getBrokerId());
            
            // Perform controller duties
            partitionRebalancer.rebalanceIfNeeded();
        }
    }

    private void resignController() {
        if (isController) {
            isController = false;
            logger.info("Broker {} resigned as cluster controller", brokerConfig.getBrokerId());
        }
    }

    public boolean isController() { return isController; }
    public ClusterMetadata getClusterMetadata() { return clusterMetadata; }
    public Map<String, Topic> getTopics() { return Map.copyOf(topics); }
    public Topic getTopic(String name) { return topics.get(name); }
    public int getBrokerId() { return brokerConfig.getBrokerId(); }
}