package com.streamflow.replication;

import com.streamflow.broker.BrokerNode;
import com.streamflow.core.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationFetcher {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationFetcher.class);
    
    private final TopicPartition partition;
    private final int leaderId;
    private final BrokerNode localBroker;
    private volatile boolean isRunning;

    public ReplicationFetcher(TopicPartition partition, int leaderId, BrokerNode localBroker) {
        this.partition = partition;
        this.leaderId = leaderId;
        this.localBroker = localBroker;
        this.isRunning = false;
    }

    public void start() {
        isRunning = true;
        logger.info("Started replication fetcher for partition {} from leader {}", partition, leaderId);
    }

    public void stop() {
        isRunning = false;
        logger.info("Stopped replication fetcher for partition {}", partition);
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void handleLeaderFailure() {
        logger.warn("Leader {} failed for partition {}, stopping fetcher", leaderId, partition);
        stop();
    }
}