package com.streamflow.storage;

public class StorageMetrics {
    private final long totalBytes;
    private final long totalMessages;
    private final long oldestMessageTimestamp;
    private final long newestMessageTimestamp;

    public StorageMetrics(long totalBytes, long totalMessages, 
                         long oldestMessageTimestamp, long newestMessageTimestamp) {
        this.totalBytes = totalBytes;
        this.totalMessages = totalMessages;
        this.oldestMessageTimestamp = oldestMessageTimestamp;
        this.newestMessageTimestamp = newestMessageTimestamp;
    }

    public long getTotalBytes() { return totalBytes; }
    public long getTotalMessages() { return totalMessages; }
    public long getOldestMessageTimestamp() { return oldestMessageTimestamp; }
    public long getNewestMessageTimestamp() { return newestMessageTimestamp; }
}