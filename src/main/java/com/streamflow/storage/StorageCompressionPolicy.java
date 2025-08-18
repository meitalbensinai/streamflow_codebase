package com.streamflow.storage;

import com.streamflow.config.ConfigurationRegistry;
import com.streamflow.core.CompressionType;
import com.streamflow.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Determines storage compression policies based on message characteristics and system configuration
 * Integrates with broker configuration through the configuration registry
 */
public class StorageCompressionPolicy {
    private static final Logger logger = LoggerFactory.getLogger(StorageCompressionPolicy.class);
    
    private final ConfigurationRegistry configRegistry;
    private final AtomicLong compressionThreshold = new AtomicLong(1024); // Default 1KB
    
    public StorageCompressionPolicy() {
        this.configRegistry = ConfigurationRegistry.getInstance();
        initializePolicy();
    }
    
    private void initializePolicy() {
        // Register as a config provider for storage compression threshold
        configRegistry.registerConfigProvider("storage.engine.compression.policy.threshold",
            () -> compressionThreshold.get());
    }
    
    public boolean shouldCompress(Message message) {
        if (message == null || message.getValue() == null) {
            return false;
        }
        
        // Get threshold from configuration registry (which might come from broker config)
        Object thresholdValue = configRegistry.getConfigValue("broker.storage.compression.threshold");
        if (thresholdValue instanceof Number) {
            long threshold = ((Number) thresholdValue).longValue();
            return message.getSizeInBytes() > threshold;
        }
        
        // Fallback to default threshold
        return message.getSizeInBytes() > compressionThreshold.get();
    }
    
    public CompressionType selectCompressionType(Message message) {
        if (!shouldCompress(message)) {
            return CompressionType.NONE;
        }
        
        // Select compression type based on message characteristics
        int messageSize = message.getSizeInBytes();
        String topic = message.getTopic();
        
        // System topics use different compression strategy
        if (topic.startsWith("__system")) {
            return CompressionType.LZ4; // Fast compression for system messages
        }
        
        // Large messages get better compression
        if (messageSize > 10240) { // 10KB
            return CompressionType.GZIP;
        }
        
        // Medium messages get balanced compression
        return CompressionType.SNAPPY;
    }
    
    public void setCompressionThreshold(long threshold) {
        long oldThreshold = compressionThreshold.getAndSet(threshold);
        logger.info("Compression threshold changed from {} to {} bytes", oldThreshold, threshold);
    }
    
    public long getCompressionThreshold() {
        return compressionThreshold.get();
    }
}