package com.streamflow.config;

import java.util.HashMap;
import java.util.Map;

public class ConfigChangeProposal {
    private final Map<String, Object> changes;
    private String reason;
    private String applicant;
    private ConfigChangePriority priority;
    
    public ConfigChangeProposal() {
        this.changes = new HashMap<>();
        this.priority = ConfigChangePriority.NORMAL;
    }
    
    public ConfigChangeProposal addChange(String key, Object value) {
        changes.put(key, value);
        return this;
    }
    
    public ConfigChangeProposal setReason(String reason) {
        this.reason = reason;
        return this;
    }
    
    public ConfigChangeProposal setApplicant(String applicant) {
        this.applicant = applicant;
        return this;
    }
    
    public ConfigChangeProposal setPriority(ConfigChangePriority priority) {
        this.priority = priority;
        return this;
    }
    
    public Map<String, Object> getChanges() { return changes; }
    public String getReason() { return reason; }
    public String getApplicant() { return applicant; }
    public ConfigChangePriority getPriority() { return priority; }
}