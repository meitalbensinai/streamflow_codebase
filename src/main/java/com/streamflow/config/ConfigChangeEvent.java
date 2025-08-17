package com.streamflow.config;

public class ConfigChangeEvent {
    private final String key;
    private final Object oldValue;
    private final Object newValue;
    private final long timestamp;
    
    public ConfigChangeEvent(String key, Object oldValue, Object newValue, long timestamp) {
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.timestamp = timestamp;
    }
    
    public String getKey() { return key; }
    public Object getOldValue() { return oldValue; }
    public Object getNewValue() { return newValue; }
    public long getTimestamp() { return timestamp; }
}