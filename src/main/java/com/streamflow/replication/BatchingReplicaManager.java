package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchingReplicaManager extends ReplicaManager {
    private static final Logger logger = LoggerFactory.getLogger(BatchingReplicaManager.class);
    
    private final MetricsCollector metricsCollector;
    private final ExecutorService replicationExecutor;
    private final AtomicBoolean isRunning;
    private final AtomicInteger activeReplications;

    public BatchingReplicaManager(TopicPartition partition, List<Integer> allReplicas, 
                                 BrokerNode localBroker, MetricsCollector metricsCollector) {
        super(partition, allReplicas, localBroker);
        this.metricsCollector = metricsCollector;
        this.replicationExecutor = Executors.newFixedThreadPool(
            Math.min(allReplicas.size(), 4), 
            r -> {
                Thread t = new Thread(r, "batch-replication-" + partition.toString());
                t.setDaemon(true);
                return t;
            }
        );
        this.isRunning = new AtomicBoolean(false);
        this.activeReplications = new AtomicInteger(0);
    }

    @Override
    public void start() {
        super.start();
        isRunning.set(true);
        logger.info("Started batching replica manager for partition {}", getPartition());
    }

    @Override
    public void stop() {
        isRunning.set(false);
        
        // Wait for active replications to complete
        int waitCount = 0;
        while (activeReplications.get() > 0 && waitCount < 50) {
            try {
                Thread.sleep(100);
                waitCount++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            replicationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        super.stop();
        logger.info("Stopped batching replica manager for partition {}", getPartition());
    }

    public void replicateBatch(MessageBatch batch) {
        if (!isRunning.get()) {
            logger.warn("Batching replica manager not running for partition {}", getPartition());
            batch.completeAllExceptionally(new IllegalStateException("Replica manager not running"));
            return;
        }

        if (batch.isEmpty()) {
            return;
        }

        activeReplications.incrementAndGet();
        
        try {
            // Replicate to all replicas in parallel
            List<CompletableFuture<ReplicationResult>> replicationFutures = new ArrayList<>();
            
            for (int replicaId : getAllReplicas()) {
                if (replicaId != localBroker.getBrokerId()) {
                    CompletableFuture<ReplicationResult> future = CompletableFuture.supplyAsync(
                        () -> replicateToReplica(replicaId, batch), 
                        replicationExecutor
                    );
                    replicationFutures.add(future);
                }
            }

            // Wait for all replications to complete
            CompletableFuture<Void> allReplications = CompletableFuture.allOf(
                replicationFutures.toArray(new CompletableFuture[0])
            );

            try {
                // Wait for replication with timeout
                allReplications.get(localBroker.getConfig().getReplicaSocketTimeoutMs(), TimeUnit.MILLISECONDS);
                
                // Collect results
                List<Integer> successfulReplicas = new ArrayList<>();
                List<Integer> failedReplicas = new ArrayList<>();
                Exception lastException = null;

                for (CompletableFuture<ReplicationResult> future : replicationFutures) {
                    try {
                        ReplicationResult result = future.get();
                        if (result.isSuccess()) {
                            successfulReplicas.add(result.getReplicaId());
                        } else {
                            failedReplicas.add(result.getReplicaId());
                            lastException = result.getException();
                        }
                    } catch (Exception e) {
                        logger.error("Error getting replication result", e);
                        lastException = e;
                    }
                }

                // Update ISR based on results
                updateISRBasedOnResults(successfulReplicas, failedReplicas);
                
                // Handle partial failures
                if (!failedReplicas.isEmpty()) {
                    metricsCollector.recordBatchPartialFailure(
                        getPartition(), failedReplicas.size(), getAllReplicas().size() - 1);
                    
                    // If majority succeeded, consider it successful
                    int totalReplicas = getAllReplicas().size() - 1; // Exclude local broker
                    int successfulCount = successfulReplicas.size();
                    
                    if (successfulCount >= (totalReplicas / 2) + 1) {
                        batch.completeAll();
                        logger.debug("Batch replication succeeded with partial failures: {} successful, {} failed", 
                                   successfulCount, failedReplicas.size());
                    } else {
                        batch.completeAllExceptionally(
                            lastException != null ? lastException : 
                            new RuntimeException("Insufficient replicas acknowledged batch"));
                        logger.error("Batch replication failed: insufficient replicas acknowledged");
                    }
                } else {
                    // All replications succeeded
                    batch.completeAll();
                    logger.debug("Batch replication completed successfully for all {} replicas", 
                               successfulReplicas.size());
                }

                // Update high watermark
                updateHighWatermark();

            } catch (TimeoutException e) {
                logger.error("Timeout during batch replication for partition {}", getPartition());
                batch.completeAllExceptionally(e);
                metricsCollector.recordError("batch_replication_timeout");
            } catch (Exception e) {
                logger.error("Error during batch replication for partition {}", getPartition(), e);
                batch.completeAllExceptionally(e);
                metricsCollector.recordError("batch_replication_error");
            }

        } finally {
            activeReplications.decrementAndGet();
        }
    }

    private ReplicationResult replicateToReplica(int replicaId, MessageBatch batch) {
        try {
            // Simulate network replication (in real implementation, this would send over network)
            logger.debug("Replicating batch of {} messages to replica {} for partition {}", 
                       batch.size(), replicaId, getPartition());
            
            // Check if replica is available
            if (!isReplicaHealthy(replicaId)) {
                throw new RuntimeException("Replica " + replicaId + " is not healthy");
            }
            
            // Simulate replication latency based on batch size
            int simulatedLatency = Math.min(10 + (batch.size() / 10), 100);
            Thread.sleep(simulatedLatency);
            
            // In real implementation:
            // 1. Serialize the batch
            // 2. Send to replica via network
            // 3. Wait for acknowledgment
            // 4. Handle retries on failure
            
            return new ReplicationResult(replicaId, true, null);
            
        } catch (Exception e) {
            logger.warn("Failed to replicate batch to replica {} for partition {}: {}", 
                       replicaId, getPartition(), e.getMessage());
            return new ReplicationResult(replicaId, false, e);
        }
    }

    private boolean isReplicaHealthy(int replicaId) {
        // In real implementation, this would check replica health
        // For testing, always return true unless it's a specific test scenario
        return true;
    }

    private void updateISRBasedOnResults(List<Integer> successfulReplicas, List<Integer> failedReplicas) {
        // Remove failed replicas from ISR
        for (int failedReplicaId : failedReplicas) {
            removeFromISR(failedReplicaId);
        }
        
        // Add successful replicas to ISR if they're not already in
        for (int successfulReplicaId : successfulReplicas) {
            if (!isInSyncReplica(successfulReplicaId)) {
                addToISR(successfulReplicaId);
            }
        }
    }

    @Override
    public void replicateMessage(Message message) {
        // Individual message replication for backward compatibility
        logger.debug("Individual message replication called - this should use batching instead");
        super.replicateMessage(message);
    }

    private static class ReplicationResult {
        private final int replicaId;
        private final boolean success;
        private final Exception exception;

        public ReplicationResult(int replicaId, boolean success, Exception exception) {
            this.replicaId = replicaId;
            this.success = success;
            this.exception = exception;
        }

        public int getReplicaId() { return replicaId; }
        public boolean isSuccess() { return success; }
        public Exception getException() { return exception; }
    }
}