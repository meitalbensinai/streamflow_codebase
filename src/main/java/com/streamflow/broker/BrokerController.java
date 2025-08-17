package com.streamflow.broker;

import com.streamflow.config.BrokerConfig;
import com.streamflow.core.Topic;
import com.streamflow.core.TopicPartition;
import com.streamflow.partition.PartitionRebalancer;
import com.streamflow.partition.LeaderElection;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.replication.ReplicationManager;
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
        this.partitionRebalancer = new PartitionRebalancer(clusterMetadata, null, brokerConfig);
        this.leaderElection = new LeaderElection(clusterMetadata, brokerConfig);
        this.scheduledExecutor = Executors.newScheduledThreadPool(2);
        this.topics = new ConcurrentHashMap<>();
        this.isController = false;
    }

    public void startup() {
        logger.info("Starting broker controller");
        
        // Startup will be handled by individual components
        
        // Start periodic tasks
        scheduledExecutor.scheduleAtFixedRate(
            this::performLeaderElection, 
            0, 
            config.getLeaderElectionIntervalMs(), 
            TimeUnit.MILLISECONDS
        );
        
        scheduledExecutor.scheduleAtFixedRate(
            this::checkPartitionHealth, 
            config.getPartitionHealthCheckIntervalMs(), 
            config.getPartitionHealthCheckIntervalMs(), 
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Broker controller started for broker {}", broker.getBrokerId());
    }

    public void shutdown() {
        logger.info("Shutting down broker controller for broker {}", broker.getBrokerId());
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Broker controller shut down for broker {}", broker.getBrokerId());
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
            logger.info("Broker {} became the cluster controller", broker.getBrokerId());
            
            // Perform controller duties
            partitionRebalancer.rebalanceIfNeeded();
        }
    }

    private void resignController() {
        if (isController) {
            isController = false;
            logger.info("Broker {} resigned as cluster controller", broker.getBrokerId());
        }
    }

    public boolean isController() { return isController; }
    public ClusterMetadata getClusterMetadata() { return clusterMetadata; }
    public Map<String, Topic> getTopics() { return Map.copyOf(topics); }
    public Topic getTopic(String name) { return topics.get(name); }
    public BrokerNode getBrokerNode() { return broker; }
}