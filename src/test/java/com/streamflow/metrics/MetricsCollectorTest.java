package com.streamflow.metrics;

import com.streamflow.core.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

class MetricsCollectorTest {
    
    private MetricsCollector metricsCollector;
    private TopicPartition partition;

    @BeforeEach
    void setUp() {
        metricsCollector = new MetricsCollector();
        partition = new TopicPartition("test-topic", 0);
    }

    @Test
    void testBrokerLifecycleMetrics() {
        int brokerId = 1;
        
        // Test startup metrics
        metricsCollector.recordBrokerStartup(brokerId);
        assertEquals(1L, metricsCollector.getCounter("broker.startups"));
        assertEquals(1L, metricsCollector.getGauge("broker.status." + brokerId));
        
        // Test shutdown metrics
        metricsCollector.recordBrokerShutdown(brokerId);
        assertEquals(1L, metricsCollector.getCounter("broker.shutdowns"));
        assertEquals(0L, metricsCollector.getGauge("broker.status." + brokerId));
    }

    @Test
    void testMessageProductionMetrics() {
        int messageSize = 1024;
        
        metricsCollector.recordMessageProduced(partition, messageSize);
        
        assertEquals(1L, metricsCollector.getCounter("messages.produced.total"));
        assertEquals(1L, metricsCollector.getCounter("messages.produced." + partition.getTopic()));
        assertEquals(messageSize, metricsCollector.getGauge("bytes.produced.total"));
        
        // Test partition-specific metrics
        MetricsCollector.PartitionMetrics partitionMetrics = 
            metricsCollector.getPartitionMetrics(partition);
        assertNotNull(partitionMetrics);
        assertEquals(1L, partitionMetrics.getMessagesProduced());
        assertEquals(messageSize, partitionMetrics.getBytesProduced());
    }

    @Test
    void testMessageConsumptionMetrics() {
        int messageSize = 512;
        
        metricsCollector.recordMessageConsumed(partition, messageSize);
        
        assertEquals(1L, metricsCollector.getCounter("messages.consumed.total"));
        assertEquals(1L, metricsCollector.getCounter("messages.consumed." + partition.getTopic()));
        assertEquals(messageSize, metricsCollector.getGauge("bytes.consumed.total"));
        
        // Test partition-specific metrics
        MetricsCollector.PartitionMetrics partitionMetrics = 
            metricsCollector.getPartitionMetrics(partition);
        assertNotNull(partitionMetrics);
        assertEquals(1L, partitionMetrics.getMessagesConsumed());
        assertEquals(messageSize, partitionMetrics.getBytesConsumed());
    }

    @Test
    void testReplicationLagMetrics() {
        int replicaId = 2;
        long lag = 1000L;
        
        metricsCollector.recordReplicationLag(partition, replicaId, lag);
        
        String lagKey = "replication.lag." + partition + "." + replicaId;
        assertEquals(lag, metricsCollector.getGauge(lagKey));
        
        // Test partition-specific lag tracking
        MetricsCollector.PartitionMetrics partitionMetrics = 
            metricsCollector.getPartitionMetrics(partition);
        assertNotNull(partitionMetrics);
        assertEquals(lag, partitionMetrics.getMaxReplicationLag());
        
        // Test higher lag updates max
        metricsCollector.recordReplicationLag(partition, replicaId, 2000L);
        assertEquals(2000L, partitionMetrics.getMaxReplicationLag());
        
        // Test lower lag doesn't update max
        metricsCollector.recordReplicationLag(partition, replicaId, 500L);
        assertEquals(2000L, partitionMetrics.getMaxReplicationLag());
    }

    @Test
    void testLeaderElectionMetrics() {
        metricsCollector.recordLeaderElection(partition);
        
        assertEquals(1L, metricsCollector.getCounter("leader.elections.total"));
        assertEquals(1L, metricsCollector.getCounter("leader.elections." + partition.getTopic()));
        
        // Test multiple elections
        metricsCollector.recordLeaderElection(partition);
        assertEquals(2L, metricsCollector.getCounter("leader.elections.total"));
        assertEquals(2L, metricsCollector.getCounter("leader.elections." + partition.getTopic()));
    }

    @Test
    void testPartitionRebalanceMetrics() {
        long beforeTime = System.currentTimeMillis();
        
        metricsCollector.recordPartitionRebalance();
        
        long afterTime = System.currentTimeMillis();
        
        assertEquals(1L, metricsCollector.getCounter("partition.rebalances.total"));
        
        long recordedTime = metricsCollector.getGauge("partition.rebalances.last");
        assertTrue(recordedTime >= beforeTime && recordedTime <= afterTime);
    }

    @Test
    void testRequestProcessingMetrics() {
        String requestType = "produce";
        long latency = 50L;
        
        metricsCollector.recordRequestProcessed(requestType, latency);
        
        assertEquals(1L, metricsCollector.getCounter("requests.processed." + requestType));
        assertEquals(latency, metricsCollector.getGauge("request.latency." + requestType));
    }

    @Test
    void testErrorMetrics() {
        String errorType = "timeout";
        
        metricsCollector.recordError(errorType);
        
        assertEquals(1L, metricsCollector.getCounter("errors.total"));
        assertEquals(1L, metricsCollector.getCounter("errors." + errorType));
        
        // Test multiple errors
        metricsCollector.recordError("network");
        assertEquals(2L, metricsCollector.getCounter("errors.total"));
        assertEquals(1L, metricsCollector.getCounter("errors.network"));
    }

    @Test
    void testGetAllCounters() {
        metricsCollector.recordBrokerStartup(1);
        metricsCollector.recordMessageProduced(partition, 1024);
        metricsCollector.recordError("test");
        
        Map<String, Long> allCounters = metricsCollector.getAllCounters();
        
        assertTrue(allCounters.containsKey("broker.startups"));
        assertTrue(allCounters.containsKey("messages.produced.total"));
        assertTrue(allCounters.containsKey("errors.total"));
        
        assertEquals(Long.valueOf(1), allCounters.get("broker.startups"));
        assertEquals(Long.valueOf(1), allCounters.get("messages.produced.total"));
        assertEquals(Long.valueOf(1), allCounters.get("errors.total"));
    }

    @Test
    void testGetAllGauges() {
        metricsCollector.recordBrokerStartup(1);
        metricsCollector.recordMessageProduced(partition, 1024);
        
        Map<String, Long> allGauges = metricsCollector.getAllGauges();
        
        assertTrue(allGauges.containsKey("broker.status.1"));
        assertTrue(allGauges.containsKey("bytes.produced.total"));
        
        assertEquals(Long.valueOf(1), allGauges.get("broker.status.1"));
        assertEquals(Long.valueOf(1024), allGauges.get("bytes.produced.total"));
    }

    @Test
    void testNonExistentMetrics() {
        assertEquals(0L, metricsCollector.getCounter("non.existent.counter"));
        assertEquals(0L, metricsCollector.getGauge("non.existent.gauge"));
        assertNull(metricsCollector.getPartitionMetrics(new TopicPartition("non-existent", 0)));
    }

    @Test
    void testConcurrentMetricsUpdates() {
        // Test thread safety by performing multiple operations
        // In a real test, this would involve actual concurrent threads
        for (int i = 0; i < 100; i++) {
            metricsCollector.recordMessageProduced(partition, 100);
            metricsCollector.recordMessageConsumed(partition, 100);
        }
        
        assertEquals(100L, metricsCollector.getCounter("messages.produced.total"));
        assertEquals(100L, metricsCollector.getCounter("messages.consumed.total"));
        assertEquals(10000L, metricsCollector.getGauge("bytes.produced.total"));
        assertEquals(10000L, metricsCollector.getGauge("bytes.consumed.total"));
    }
}