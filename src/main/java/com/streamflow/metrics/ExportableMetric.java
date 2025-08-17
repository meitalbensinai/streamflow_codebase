package com.streamflow.metrics;

public class ExportableMetric {
    private final String name;
    private final Object value;
    private final long timestamp;
    
    public ExportableMetric(String name, Object value, long timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public String getName() { return name; }
    public Object getValue() { return value; }
    public long getTimestamp() { return timestamp; }
}