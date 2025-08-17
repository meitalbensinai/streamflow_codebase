package com.streamflow.config;

/**
 * Configuration class for batching behavior in replication operations.
 * Provides configuration parameters for controlling batch size, timeout, and other batching characteristics.
 */
public class BatchingConfiguration {
    private final int batchSize;
    private final long batchTimeoutMs;
    private final long batchMaxBytes;
    private final boolean enabled;

    public BatchingConfiguration(int batchSize, long batchTimeoutMs, long batchMaxBytes, boolean enabled) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.batchMaxBytes = batchMaxBytes;
        this.enabled = enabled;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public long getBatchMaxBytes() {
        return batchMaxBytes;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "BatchingConfiguration{" +
                "batchSize=" + batchSize +
                ", batchTimeoutMs=" + batchTimeoutMs +
                ", batchMaxBytes=" + batchMaxBytes +
                ", enabled=" + enabled +
                '}';
    }
}