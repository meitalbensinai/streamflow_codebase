package com.streamflow.security;

import java.util.List;
import java.util.Map;

public class SecurityAuditReport {
    private final List<AuthenticationEvent> authenticationEvents;
    private final List<AuthorizationDecision> authorizationDecisions;
    private final List<SecurityViolation> securityViolations;
    private final Map<String, Object> metadata;
    
    public SecurityAuditReport(List<AuthenticationEvent> authenticationEvents,
                              List<AuthorizationDecision> authorizationDecisions,
                              List<SecurityViolation> securityViolations,
                              Map<String, Object> metadata) {
        this.authenticationEvents = authenticationEvents;
        this.authorizationDecisions = authorizationDecisions;
        this.securityViolations = securityViolations;
        this.metadata = metadata;
    }
    
    public List<AuthenticationEvent> getAuthenticationEvents() { return authenticationEvents; }
    public List<AuthorizationDecision> getAuthorizationDecisions() { return authorizationDecisions; }
    public List<SecurityViolation> getSecurityViolations() { return securityViolations; }
    public Map<String, Object> getMetadata() { return metadata; }
    
    public static class AuthenticationEvent {
        private final String clientId;
        private final boolean success;
        private final long timestamp;
        
        public AuthenticationEvent(String clientId, boolean success, long timestamp) {
            this.clientId = clientId;
            this.success = success;
            this.timestamp = timestamp;
        }
        
        public String getClientId() { return clientId; }
        public boolean isSuccess() { return success; }
        public long getTimestamp() { return timestamp; }
    }
    
    public static class AuthorizationDecision {
        private final String sessionId;
        private final String resource;
        private final String operation;
        private final boolean granted;
        private final long timestamp;
        
        public AuthorizationDecision(String sessionId, String resource, String operation, 
                                   boolean granted, long timestamp) {
            this.sessionId = sessionId;
            this.resource = resource;
            this.operation = operation;
            this.granted = granted;
            this.timestamp = timestamp;
        }
        
        public String getSessionId() { return sessionId; }
        public String getResource() { return resource; }
        public String getOperation() { return operation; }
        public boolean isGranted() { return granted; }
        public long getTimestamp() { return timestamp; }
    }
    
    public static class SecurityViolation {
        private final String type;
        private final String description;
        private final String clientId;
        private final long timestamp;
        
        public SecurityViolation(String type, String description, String clientId, long timestamp) {
            this.type = type;
            this.description = description;
            this.clientId = clientId;
            this.timestamp = timestamp;
        }
        
        public String getType() { return type; }
        public String getDescription() { return description; }
        public String getClientId() { return clientId; }
        public long getTimestamp() { return timestamp; }
    }
}