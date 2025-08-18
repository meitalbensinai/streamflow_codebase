package com.streamflow.replication;

import com.streamflow.core.Message;
import com.streamflow.core.TopicPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MessageBatch {
    private final TopicPartition partition;
    private final List<BatchMessage> messages;
    private final AtomicLong totalBytes;
    private final Instant creationTime;
    private final AtomicInteger maxBatchSize;
    private final AtomicLong maxBatchBytes;
    private volatile boolean sealed;

    public MessageBatch(TopicPartition partition, int maxBatchSize, long maxBatchBytes) {
        this.partition = partition;
        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.totalBytes = new AtomicLong(0);
        this.creationTime = Instant.now();
        this.maxBatchSize = new AtomicInteger(maxBatchSize);
        this.maxBatchBytes = new AtomicLong(maxBatchBytes);
        this.sealed = false;
    }

    public boolean tryAdd(Message message, CompletableFuture<Void> ackFuture) {
        if (sealed || !partition.equals(new TopicPartition(message.getTopic(), message.getPartition()))) {
            return false;
        }

        synchronized (this) {
            if (sealed) {
                return false;
            }

            int messageSize = message.getSizeInBytes();
            
            if (messages.size() >= maxBatchSize.get() || 
                (totalBytes.get() + messageSize) > maxBatchBytes.get()) {
                return false;
            }

            messages.add(new BatchMessage(message, ackFuture));
            totalBytes.addAndGet(messageSize);
            return true;
        }
    }

    public void seal() {
        this.sealed = true;
    }

    public boolean isSealed() {
        return sealed;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    public int size() {
        return messages.size();
    }

    public long getTotalBytes() {
        return totalBytes.get();
    }

    public TopicPartition getPartition() {
        return partition;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public List<BatchMessage> getMessages() {
        return new ArrayList<>(messages);
    }

    public boolean shouldFlush(long timeoutMs) {
        if (isEmpty()) {
            return false;
        }
        
        return sealed || 
               messages.size() >= maxBatchSize.get() ||
               totalBytes.get() >= maxBatchBytes.get() ||
               (Instant.now().toEpochMilli() - creationTime.toEpochMilli()) >= timeoutMs;
    }

    public void completeAll() {
        for (BatchMessage batchMessage : messages) {
            batchMessage.getAckFuture().complete(null);
        }
    }

    public void completeAllExceptionally(Throwable throwable) {
        for (BatchMessage batchMessage : messages) {
            batchMessage.getAckFuture().completeExceptionally(throwable);
        }
    }

    public void completePartial(List<Integer> successfulReplicaIds, List<Integer> failedReplicaIds, Throwable error) {
        if (failedReplicaIds.isEmpty()) {
            completeAll();
        } else {
            for (BatchMessage batchMessage : messages) {
                if (error != null) {
                    batchMessage.getAckFuture().completeExceptionally(error);
                } else {
                    batchMessage.getAckFuture().complete(null);
                }
            }
        }
    }

    public double getEfficiencyRatio() {
        if (messages.isEmpty()) {
            return 0.0;
        }
        return (double) messages.size() / maxBatchSize.get();
    }

    public static class BatchMessage {
        private final Message message;
        private final CompletableFuture<Void> ackFuture;

        public BatchMessage(Message message, CompletableFuture<Void> ackFuture) {
            this.message = message;
            this.ackFuture = ackFuture;
        }

        public Message getMessage() {
            return message;
        }

        public CompletableFuture<Void> getAckFuture() {
            return ackFuture;
        }
    }

    @Override
    public String toString() {
        return "MessageBatch{" +
               "partition=" + partition +
               ", messageCount=" + messages.size() +
               ", totalBytes=" + totalBytes.get() +
               ", creationTime=" + creationTime +
               ", sealed=" + sealed +
               '}';
    }
}