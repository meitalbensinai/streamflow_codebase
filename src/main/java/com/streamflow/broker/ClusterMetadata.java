package com.streamflow.broker;

import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClusterMetadata {
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);
    
    private final Map<Integer, BrokerNode> aliveBrokers;
    private final Map<TopicPartition, PartitionMetadata> partitionMetadata;
    private final Map<TopicPartition, List<Integer>> partitionReplicas;
    private final Map<TopicPartition, Set<Integer>> inSyncReplicas;
    private final ReadWriteLock lock;
    
    private volatile Integer controllerId;

    public ClusterMetadata() {
        this.aliveBrokers = new ConcurrentHashMap<>();
        this.partitionMetadata = new ConcurrentHashMap<>();
        this.partitionReplicas = new ConcurrentHashMap<>();
        this.inSyncReplicas = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.controllerId = null;
    }

    public void registerBroker(BrokerNode broker) {
        lock.writeLock().lock();
        try {
            aliveBrokers.put(broker.getBrokerId(), broker);
            logger.info("Registered broker {} at {}:{}", 
                       broker.getBrokerId(), broker.getHost(), broker.getPort());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unregisterBroker(int brokerId) {
        lock.writeLock().lock();
        try {
            BrokerNode removed = aliveBrokers.remove(brokerId);
            if (removed != null) {
                handleBrokerFailure(brokerId);
                logger.info("Unregistered broker {}", brokerId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updatePartitionLeader(TopicPartition partition, int leaderId) {
        lock.writeLock().lock();
        try {
            PartitionMetadata metadata = partitionMetadata.get(partition);
            if (metadata != null) {
                metadata.setLeader(leaderId);
                logger.debug("Updated leader for partition {} to broker {}", partition, leaderId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updatePartitionReplicas(TopicPartition partition, List<Integer> replicas) {
        lock.writeLock().lock();
        try {
            partitionReplicas.put(partition, new ArrayList<>(replicas));
            // Initialize ISR with all replicas initially
            inSyncReplicas.put(partition, new HashSet<>(replicas));
            
            PartitionMetadata metadata = partitionMetadata.get(partition);
            if (metadata == null) {
                metadata = new PartitionMetadata(partition, replicas.get(0), replicas);
                partitionMetadata.put(partition, metadata);
            } else {
                metadata.setReplicas(replicas);
            }
            
            logger.debug("Updated replicas for partition {} to {}", partition, replicas);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateInSyncReplicas(TopicPartition partition, Set<Integer> isr) {
        lock.writeLock().lock();
        try {
            inSyncReplicas.put(partition, new HashSet<>(isr));
            PartitionMetadata metadata = partitionMetadata.get(partition);
            if (metadata != null) {
                metadata.setInSyncReplicas(new ArrayList<>(isr));
            }
            logger.debug("Updated ISR for partition {} to {}", partition, isr);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void handleBrokerFailure(int failedBrokerId) {
        // Find all partitions where this broker was leader
        List<TopicPartition> affectedPartitions = new ArrayList<>();
        
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : partitionMetadata.entrySet()) {
            PartitionMetadata metadata = entry.getValue();
            if (metadata.getLeader() == failedBrokerId) {
                affectedPartitions.add(entry.getKey());
            }
            
            // Remove from ISR
            Set<Integer> isr = inSyncReplicas.get(entry.getKey());
            if (isr != null) {
                isr.remove(failedBrokerId);
            }
        }
        
        logger.warn("Broker {} failed, affected partitions: {}", failedBrokerId, affectedPartitions);
    }

    public BrokerNode getBroker(int brokerId) {
        lock.readLock().lock();
        try {
            return aliveBrokers.get(brokerId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<BrokerNode> getAliveBrokers() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(aliveBrokers.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public PartitionMetadata getPartitionMetadata(TopicPartition partition) {
        lock.readLock().lock();
        try {
            return partitionMetadata.get(partition);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Integer> getReplicas(TopicPartition partition) {
        lock.readLock().lock();
        try {
            List<Integer> replicas = partitionReplicas.get(partition);
            return replicas != null ? new ArrayList<>(replicas) : new ArrayList<>();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<Integer> getInSyncReplicas(TopicPartition partition) {
        lock.readLock().lock();
        try {
            Set<Integer> isr = inSyncReplicas.get(partition);
            return isr != null ? new HashSet<>(isr) : new HashSet<>();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setController(int brokerId) {
        this.controllerId = brokerId;
        logger.info("Controller set to broker {}", brokerId);
    }

    public Integer getController() {
        return controllerId;
    }

    public boolean isBrokerAlive(int brokerId) {
        lock.readLock().lock();
        try {
            return aliveBrokers.containsKey(brokerId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getAliveBrokerCount() {
        lock.readLock().lock();
        try {
            return aliveBrokers.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}