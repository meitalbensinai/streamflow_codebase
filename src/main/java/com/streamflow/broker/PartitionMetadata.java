package com.streamflow.broker;

import com.streamflow.core.TopicPartition;

import java.util.List;
import java.util.Objects;

public class PartitionMetadata {
    private final TopicPartition topicPartition;
    private volatile int leader;
    private volatile List<Integer> replicas;
    private volatile List<Integer> inSyncReplicas;
    private volatile long highWatermark;
    private volatile long logEndOffset;

    public PartitionMetadata(TopicPartition topicPartition, int leader, List<Integer> replicas) {
        this.topicPartition = topicPartition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = replicas;
        this.highWatermark = 0L;
        this.logEndOffset = 0L;
    }

    public TopicPartition getTopicPartition() { return topicPartition; }
    
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    
    public List<Integer> getReplicas() { return replicas; }
    public void setReplicas(List<Integer> replicas) { this.replicas = replicas; }
    
    public List<Integer> getInSyncReplicas() { return inSyncReplicas; }
    public void setInSyncReplicas(List<Integer> inSyncReplicas) { this.inSyncReplicas = inSyncReplicas; }
    
    public long getHighWatermark() { return highWatermark; }
    public void setHighWatermark(long highWatermark) { this.highWatermark = highWatermark; }
    
    public long getLogEndOffset() { return logEndOffset; }
    public void setLogEndOffset(long logEndOffset) { this.logEndOffset = logEndOffset; }

    public boolean isInSync(int brokerId) {
        return inSyncReplicas.contains(brokerId);
    }

    public boolean isReplica(int brokerId) {
        return replicas.contains(brokerId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionMetadata that = (PartitionMetadata) o;
        return Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition);
    }

    @Override
    public String toString() {
        return "PartitionMetadata{" +
               "topicPartition=" + topicPartition +
               ", leader=" + leader +
               ", replicas=" + replicas +
               ", inSyncReplicas=" + inSyncReplicas +
               ", highWatermark=" + highWatermark +
               ", logEndOffset=" + logEndOffset +
               '}';
    }
}