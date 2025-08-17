package com.streamflow.security;

import java.util.Map;

public class HealthStatus {
    public enum Status {
        HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN
    }
    
    private final Status overallStatus;
    private final Map<String, Status> componentStatuses;
    private final String description;
    
    public HealthStatus(Status overallStatus, Map<String, Status> componentStatuses, String description) {
        this.overallStatus = overallStatus;
        this.componentStatuses = componentStatuses;
        this.description = description;
    }
    
    public Status getOverallStatus() { return overallStatus; }
    public Map<String, Status> getComponentStatuses() { return componentStatuses; }
    public String getDescription() { return description; }
}