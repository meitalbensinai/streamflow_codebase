package com.streamflow.config;

import java.util.Map;

public class ConfigurationSnapshot {
    private final String snapshotId;
    private final long timestamp;
    private final Map<String, Object> configurationData;
    private final String reason;
    
    public ConfigurationSnapshot(String snapshotId, long timestamp, 
                                Map<String, Object> configurationData, String reason) {
        this.snapshotId = snapshotId;
        this.timestamp = timestamp;
        this.configurationData = configurationData;
        this.reason = reason;
    }
    
    public String getSnapshotId() { return snapshotId; }
    public long getTimestamp() { return timestamp; }
    public Map<String, Object> getConfigurationData() { return configurationData; }
    public String getReason() { return reason; }
    
    public Object getConfigValue(String key) {
        return configurationData.get(key);
    }
}