package com.streamflow.config;

import java.util.HashMap;
import java.util.Map;

public class ConfigChangeRequest {
    private final Map<String, Object> changes;
    
    public ConfigChangeRequest() {
        this.changes = new HashMap<>();
    }
    
    public ConfigChangeRequest addChange(String key, Object value) {
        changes.put(key, value);
        return this;
    }
    
    public Map<String, Object> getChanges() { return changes; }
}