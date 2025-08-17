package com.streamflow.broker;

import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;
import com.streamflow.metrics.MetricsCollector;
import com.streamflow.security.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RequestProcessor.class);
    
    private final BrokerNode broker;
    private final PartitionManager partitionManager;
    private final MetricsCollector metricsCollector;

    public RequestProcessor(BrokerNode broker, PartitionManager partitionManager, 
                           MetricsCollector metricsCollector) {
        this.broker = broker;
        this.partitionManager = partitionManager;
        this.metricsCollector = metricsCollector;
    }

    public void startup() {
        logger.info("Starting request processor for broker {}", broker.getBrokerId());
    }

    public void shutdown() {
        logger.info("Shutting down request processor for broker {}", broker.getBrokerId());
    }

    public ProduceResponse handleProduceRequest(ProduceRequest request) {
        try {
            for (ProduceRequest.TopicData topicData : request.getTopicData()) {
                for (ProduceRequest.PartitionData partitionData : topicData.getPartitionData()) {
                    TopicPartition partition = new TopicPartition(
                        topicData.getTopic(), 
                        partitionData.getPartition()
                    );
                    
                    if (!broker.isLeaderFor(partition)) {
                        return ProduceResponse.notLeaderError(partition);
                    }
                    
                    for (Message message : partitionData.getMessages()) {
                        long offset = partitionManager.appendMessage(partition, message);
                        metricsCollector.recordMessageProduced(partition, message.getValue().length);
                    }
                }
            }
            return ProduceResponse.success();
        } catch (Exception e) {
            logger.error("Error handling produce request", e);
            return ProduceResponse.error(e.getMessage());
        }
    }

    public FetchResponse handleFetchRequest(FetchRequest request) {
        // Implementation would handle consumer fetch requests
        return FetchResponse.empty();
    }

    // Inner classes for request/response types
    public static class ProduceRequest {
        private final TopicData[] topicData;
        
        public ProduceRequest(TopicData[] topicData) {
            this.topicData = topicData;
        }
        
        public TopicData[] getTopicData() { return topicData; }
        
        public static class TopicData {
            private final String topic;
            private final PartitionData[] partitionData;
            
            public TopicData(String topic, PartitionData[] partitionData) {
                this.topic = topic;
                this.partitionData = partitionData;
            }
            
            public String getTopic() { return topic; }
            public PartitionData[] getPartitionData() { return partitionData; }
        }
        
        public static class PartitionData {
            private final int partition;
            private final Message[] messages;
            
            public PartitionData(int partition, Message[] messages) {
                this.partition = partition;
                this.messages = messages;
            }
            
            public int getPartition() { return partition; }
            public Message[] getMessages() { return messages; }
        }
    }

    public static class ProduceResponse {
        private final boolean success;
        private final String errorMessage;
        private final TopicPartition errorPartition;
        
        private ProduceResponse(boolean success, String errorMessage, TopicPartition errorPartition) {
            this.success = success;
            this.errorMessage = errorMessage;
            this.errorPartition = errorPartition;
        }
        
        public static ProduceResponse success() {
            return new ProduceResponse(true, null, null);
        }
        
        public static ProduceResponse notLeaderError(TopicPartition partition) {
            return new ProduceResponse(false, "NOT_LEADER_FOR_PARTITION", partition);
        }
        
        public static ProduceResponse error(String message) {
            return new ProduceResponse(false, message, null);
        }
        
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
        public TopicPartition getErrorPartition() { return errorPartition; }
    }

    public static class FetchRequest {
        // Simplified for now
    }

    public static class FetchResponse {
        public static FetchResponse empty() {
            return new FetchResponse();
        }
    }
}