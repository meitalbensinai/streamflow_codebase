package com.streamflow.metrics;

import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final Map<String, AtomicLong> counters;
    private final Map<String, AtomicLong> gauges;
    private final Map<TopicPartition, PartitionMetrics> partitionMetrics;

    public MetricsCollector() {
        this.counters = new ConcurrentHashMap<>();
        this.gauges = new ConcurrentHashMap<>();
        this.partitionMetrics = new ConcurrentHashMap<>();
    }

    public void recordBrokerStartup(int brokerId) {
        increment("broker.startups");
        setGauge("broker.status." + brokerId, 1);
        logger.info("Recorded broker startup for broker {}", brokerId);
    }

    public void recordBrokerShutdown(int brokerId) {
        increment("broker.shutdowns");
        setGauge("broker.status." + brokerId, 0);
        logger.info("Recorded broker shutdown for broker {}", brokerId);
    }

    public void recordMessageProduced(TopicPartition partition, int messageSize) {
        increment("messages.produced.total");
        increment("messages.produced." + partition.getTopic());
        addToGauge("bytes.produced.total", messageSize);
        
        PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            partition, p -> new PartitionMetrics()
        );
        metrics.messagesProduced.incrementAndGet();
        metrics.bytesProduced.addAndGet(messageSize);
    }

    public void recordMessageConsumed(TopicPartition partition, int messageSize) {
        increment("messages.consumed.total");
        increment("messages.consumed." + partition.getTopic());
        addToGauge("bytes.consumed.total", messageSize);
        
        PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            partition, p -> new PartitionMetrics()
        );
        metrics.messagesConsumed.incrementAndGet();
        metrics.bytesConsumed.addAndGet(messageSize);
    }

    public void recordReplicationLag(TopicPartition partition, int replicaId, long lag) {
        setGauge("replication.lag." + partition + "." + replicaId, lag);
        
        PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            partition, p -> new PartitionMetrics()
        );
        metrics.maxReplicationLag.set(Math.max(metrics.maxReplicationLag.get(), lag));
    }

    public void recordLeaderElection(TopicPartition partition) {
        increment("leader.elections.total");
        increment("leader.elections." + partition.getTopic());
    }

    public void recordPartitionRebalance() {
        increment("partition.rebalances.total");
        setGauge("partition.rebalances.last", System.currentTimeMillis());
    }

    public void recordRequestProcessed(String requestType, long latencyMs) {
        increment("requests.processed." + requestType);
        setGauge("request.latency." + requestType, latencyMs);
    }

    public void recordError(String errorType) {
        increment("errors.total");
        increment("errors." + errorType);
    }

    public void recordBatchCreated(TopicPartition partition) {
        increment("replication.batches.created.total");
        increment("replication.batches.created." + partition.getTopic());
    }

    public void recordBatchFlushed(TopicPartition partition, int messageCount, long batchSizeBytes, 
                                   long batchAgeMs, double efficiencyRatio) {
        increment("replication.batches.flushed.total");
        increment("replication.batches.flushed." + partition.getTopic());
        
        addToGauge("replication.batch.messages.total", messageCount);
        addToGauge("replication.batch.bytes.total", batchSizeBytes);
        setGauge("replication.batch.age.last", batchAgeMs);
        setGauge("replication.batch.efficiency.last", (long)(efficiencyRatio * 100));
        
        PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            partition, p -> new PartitionMetrics()
        );
        metrics.batchesFlushed.incrementAndGet();
        metrics.totalBatchedMessages.addAndGet(messageCount);
        metrics.averageBatchSize.set(calculateAverageBatchSize(partition));
    }

    public void recordBatchTimeout(TopicPartition partition, int messageCount) {
        increment("replication.batch.timeouts.total");
        increment("replication.batch.timeouts." + partition.getTopic());
        
        PartitionMetrics metrics = partitionMetrics.computeIfAbsent(
            partition, p -> new PartitionMetrics()
        );
        metrics.batchTimeouts.incrementAndGet();
    }

    public void recordBatchReplicationLatency(TopicPartition partition, long latencyMs) {
        setGauge("replication.batch.latency." + partition, latencyMs);
        setGauge("replication.batch.latency.last", latencyMs);
    }

    public void recordBatchPartialFailure(TopicPartition partition, int failedReplicas, int totalReplicas) {
        increment("replication.batch.partial.failures.total");
        setGauge("replication.batch.failed.replicas.last", failedReplicas);
        setGauge("replication.batch.success.ratio.last", 
                (long)(((double)(totalReplicas - failedReplicas) / totalReplicas) * 100));
    }

    private long calculateAverageBatchSize(TopicPartition partition) {
        PartitionMetrics metrics = partitionMetrics.get(partition);
        if (metrics != null && metrics.batchesFlushed.get() > 0) {
            return metrics.totalBatchedMessages.get() / metrics.batchesFlushed.get();
        }
        return 0L;
    }

    private void increment(String key) {
        counters.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    private void setGauge(String key, long value) {
        gauges.computeIfAbsent(key, k -> new AtomicLong(0)).set(value);
    }

    private void addToGauge(String key, long value) {
        gauges.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(value);
    }

    public long getCounter(String key) {
        AtomicLong counter = counters.get(key);
        return counter != null ? counter.get() : 0L;
    }

    public long getGauge(String key) {
        AtomicLong gauge = gauges.get(key);
        return gauge != null ? gauge.get() : 0L;
    }

    public PartitionMetrics getPartitionMetrics(TopicPartition partition) {
        return partitionMetrics.get(partition);
    }

    public Map<String, Long> getAllCounters() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }

    public Map<String, Long> getAllGauges() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        gauges.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }

    public static class PartitionMetrics {
        public final AtomicLong messagesProduced = new AtomicLong(0);
        public final AtomicLong messagesConsumed = new AtomicLong(0);
        public final AtomicLong bytesProduced = new AtomicLong(0);
        public final AtomicLong bytesConsumed = new AtomicLong(0);
        public final AtomicLong maxReplicationLag = new AtomicLong(0);
        
        // Batching metrics
        public final AtomicLong batchesFlushed = new AtomicLong(0);
        public final AtomicLong totalBatchedMessages = new AtomicLong(0);
        public final AtomicLong batchTimeouts = new AtomicLong(0);
        public final AtomicLong averageBatchSize = new AtomicLong(0);
        
        public long getMessagesProduced() { return messagesProduced.get(); }
        public long getMessagesConsumed() { return messagesConsumed.get(); }
        public long getBytesProduced() { return bytesProduced.get(); }
        public long getBytesConsumed() { return bytesConsumed.get(); }
        public long getMaxReplicationLag() { return maxReplicationLag.get(); }
        public long getBatchesFlushed() { return batchesFlushed.get(); }
        public long getTotalBatchedMessages() { return totalBatchedMessages.get(); }
        public long getBatchTimeouts() { return batchTimeouts.get(); }
        public long getAverageBatchSize() { return averageBatchSize.get(); }
    }
}