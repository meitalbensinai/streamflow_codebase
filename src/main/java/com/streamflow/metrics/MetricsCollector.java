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
        
        public long getMessagesProduced() { return messagesProduced.get(); }
        public long getMessagesConsumed() { return messagesConsumed.get(); }
        public long getBytesProduced() { return bytesProduced.get(); }
        public long getBytesConsumed() { return bytesConsumed.get(); }
        public long getMaxReplicationLag() { return maxReplicationLag.get(); }
    }
}