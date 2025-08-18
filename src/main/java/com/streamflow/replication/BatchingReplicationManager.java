package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchingReplicationManager extends ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(BatchingReplicationManager.class);
    
    private final Map<TopicPartition, MessageBatch> activeBatches;
    private final Map<TopicPartition, BatchingReplicaManager> batchingReplicaManagers;
    private final ScheduledExecutorService batchFlushExecutor;
    private final MetricsCollector metricsCollector;
    private final AtomicBoolean isShuttingDown;
    private volatile BrokerNode localBroker;

    public BatchingReplicationManager(MetricsCollector metricsCollector) {
        super();
        this.activeBatches = new ConcurrentHashMap<>();
        this.batchingReplicaManagers = new ConcurrentHashMap<>();
        this.batchFlushExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "batch-flush-thread");
            t.setDaemon(true);
            return t;
        });
        this.metricsCollector = metricsCollector;
        this.isShuttingDown = new AtomicBoolean(false);
    }

    @Override
    public void initialize(BrokerNode broker) {
        super.initialize(broker);
        this.localBroker = broker;
        
        if (broker.getConfig().isReplicationBatchingEnabled()) {
            startBatchFlushScheduler();
            logger.info("Initialized batching replication manager for broker {} with batching enabled", 
                       broker.getBrokerId());
        } else {
            logger.info("Initialized batching replication manager for broker {} with batching disabled", 
                       broker.getBrokerId());
        }
    }

    @Override
    public void becomeLeaderForPartition(TopicPartition partition, List<Integer> replicaIds) {
        logger.info("Becoming leader for partition {} with replicas {}", partition, replicaIds);
        
        // Stop any existing fetcher
        ReplicationFetcher fetcher = replicationFetchers.remove(partition);
        if (fetcher != null) {
            fetcher.stop();
        }

        // Flush any existing batch for this partition
        flushBatchIfExists(partition);

        if (localBroker.getConfig().isReplicationBatchingEnabled()) {
            // Create batching replica manager
            BatchingReplicaManager batchingReplicaManager = new BatchingReplicaManager(
                partition, replicaIds, localBroker, metricsCollector);
            batchingReplicaManagers.put(partition, batchingReplicaManager);
            batchingReplicaManager.start();
        } else {
            // Fall back to regular replica manager
            super.becomeLeaderForPartition(partition, replicaIds);
        }
    }

    @Override
    public void becomeFollowerForPartition(TopicPartition partition, int leaderId) {
        // Flush any existing batch
        flushBatchIfExists(partition);
        
        // Remove batching replica manager
        BatchingReplicaManager batchingReplicaManager = batchingReplicaManagers.remove(partition);
        if (batchingReplicaManager != null) {
            batchingReplicaManager.stop();
        }
        
        // Use regular behavior for followers
        super.becomeFollowerForPartition(partition, leaderId);
    }

    @Override
    public void removePartition(TopicPartition partition) {
        // Flush any existing batch
        flushBatchIfExists(partition);
        
        // Remove batching replica manager
        BatchingReplicaManager batchingReplicaManager = batchingReplicaManagers.remove(partition);
        if (batchingReplicaManager != null) {
            batchingReplicaManager.stop();
        }
        
        super.removePartition(partition);
    }

    @Override
    public void replicateMessage(TopicPartition partition, Message message) {
        if (!localBroker.getConfig().isReplicationBatchingEnabled()) {
            // Fall back to regular replication
            super.replicateMessage(partition, message);
            return;
        }

        BatchingReplicaManager batchingReplicaManager = batchingReplicaManagers.get(partition);
        if (batchingReplicaManager == null) {
            logger.warn("No batching replica manager found for partition {} when trying to replicate message", partition);
            return;
        }

        try {
            // Add to batch with async acknowledgment
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            addToBatch(partition, message, ackFuture);
            
            // Wait for acknowledgment (this maintains the synchronous API)
            ackFuture.get(localBroker.getConfig().getReplicaSocketTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.error("Timeout waiting for batch replication acknowledgment for partition {}", partition);
            metricsCollector.recordError("batch_replication_timeout");
        } catch (Exception e) {
            logger.error("Error during batch replication for partition {}", partition, e);
            metricsCollector.recordError("batch_replication_error");
        }
    }

    public CompletableFuture<Void> replicateMessageAsync(TopicPartition partition, Message message) {
        if (!localBroker.getConfig().isReplicationBatchingEnabled()) {
            // Fall back to regular replication with immediate completion
            super.replicateMessage(partition, message);
            return CompletableFuture.completedFuture(null);
        }

        BatchingReplicaManager batchingReplicaManager = batchingReplicaManagers.get(partition);
        if (batchingReplicaManager == null) {
            logger.warn("No batching replica manager found for partition {} when trying to replicate message", partition);
            return CompletableFuture.failedFuture(new IllegalStateException("No replica manager found"));
        }

        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        addToBatch(partition, message, ackFuture);
        return ackFuture;
    }

    private void addToBatch(TopicPartition partition, Message message, CompletableFuture<Void> ackFuture) {
        MessageBatch batch = activeBatches.computeIfAbsent(partition, p -> {
            MessageBatch newBatch = new MessageBatch(
                p, 
                localBroker.getConfig().getReplicationBatchSize(),
                localBroker.getConfig().getReplicationBatchMaxBytes()
            );
            metricsCollector.recordBatchCreated(p);
            return newBatch;
        });

        synchronized (batch) {
            if (!batch.tryAdd(message, ackFuture)) {
                // Current batch is full, flush it and create a new one
                flushBatch(partition, batch, "size_limit");
                
                MessageBatch newBatch = new MessageBatch(
                    partition,
                    localBroker.getConfig().getReplicationBatchSize(),
                    localBroker.getConfig().getReplicationBatchMaxBytes()
                );
                activeBatches.put(partition, newBatch);
                metricsCollector.recordBatchCreated(partition);
                
                if (!newBatch.tryAdd(message, ackFuture)) {
                    // Single message is too large for batch
                    logger.warn("Message too large for batch, falling back to individual replication");
                    super.replicateMessage(partition, message);
                    ackFuture.complete(null);
                }
            }
        }
    }

    private void startBatchFlushScheduler() {
        long timeoutMs = localBroker.getConfig().getReplicationBatchTimeoutMs();
        
        batchFlushExecutor.scheduleWithFixedDelay(() -> {
            if (isShuttingDown.get()) {
                return;
            }
            
            try {
                flushTimeoutBatches();
            } catch (Exception e) {
                logger.error("Error during scheduled batch flush", e);
            }
        }, timeoutMs / 2, timeoutMs / 4, TimeUnit.MILLISECONDS);
    }

    private void flushTimeoutBatches() {
        long timeoutMs = localBroker.getConfig().getReplicationBatchTimeoutMs();
        
        for (Map.Entry<TopicPartition, MessageBatch> entry : activeBatches.entrySet()) {
            TopicPartition partition = entry.getKey();
            MessageBatch batch = entry.getValue();
            
            if (batch.shouldFlush(timeoutMs)) {
                synchronized (batch) {
                    if (batch.shouldFlush(timeoutMs)) {
                        activeBatches.remove(partition);
                        
                        if (batch.isEmpty()) {
                            continue;
                        }
                        
                        String flushReason = (Instant.now().toEpochMilli() - batch.getCreationTime().toEpochMilli()) >= timeoutMs 
                            ? "timeout" : "size_limit";
                        flushBatch(partition, batch, flushReason);
                    }
                }
            }
        }
    }

    private void flushBatchIfExists(TopicPartition partition) {
        MessageBatch batch = activeBatches.remove(partition);
        if (batch != null && !batch.isEmpty()) {
            synchronized (batch) {
                if (!batch.isEmpty()) {
                    flushBatch(partition, batch, "partition_change");
                }
            }
        }
    }

    private void flushBatch(TopicPartition partition, MessageBatch batch, String reason) {
        if (batch.isEmpty()) {
            return;
        }

        batch.seal();
        
        long startTime = System.currentTimeMillis();
        long batchAge = startTime - batch.getCreationTime().toEpochMilli();
        
        if ("timeout".equals(reason)) {
            metricsCollector.recordBatchTimeout(partition, batch.size());
        }
        
        BatchingReplicaManager batchingReplicaManager = batchingReplicaManagers.get(partition);
        if (batchingReplicaManager != null) {
            try {
                batchingReplicaManager.replicateBatch(batch);
                
                long latency = System.currentTimeMillis() - startTime;
                metricsCollector.recordBatchFlushed(partition, batch.size(), batch.getTotalBytes(), 
                                                   batchAge, batch.getEfficiencyRatio());
                metricsCollector.recordBatchReplicationLatency(partition, latency);
                
                logger.debug("Flushed batch for partition {} with {} messages, {} bytes, reason: {}, latency: {}ms",
                           partition, batch.size(), batch.getTotalBytes(), reason, latency);
            } catch (Exception e) {
                logger.error("Failed to flush batch for partition {}", partition, e);
                batch.completeAllExceptionally(e);
                metricsCollector.recordError("batch_flush_error");
            }
        } else {
            logger.error("No batching replica manager found for partition {} during batch flush", partition);
            batch.completeAllExceptionally(new IllegalStateException("No replica manager found"));
        }
    }

    @Override
    public void shutdown() {
        isShuttingDown.set(true);
        
        // Flush all remaining batches
        logger.info("Flushing remaining batches during shutdown");
        for (TopicPartition partition : List.copyOf(activeBatches.keySet())) {
            flushBatchIfExists(partition);
        }
        
        // Stop batch flush executor
        batchFlushExecutor.shutdown();
        try {
            if (!batchFlushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                batchFlushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            batchFlushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Stop all batching replica managers
        for (BatchingReplicaManager batchingReplicaManager : batchingReplicaManagers.values()) {
            batchingReplicaManager.stop();
        }
        batchingReplicaManagers.clear();
        
        super.shutdown();
        logger.info("Batching replication manager shutdown completed");
    }
}