package com.streamflow.security;

import java.util.Map;

public class SecurityContext {
    private final String sessionId;
    private final String clientId;
    private final String authenticationMethod;
    private final boolean authenticated;
    private final Map<String, Object> attributes;
    
    public SecurityContext(String sessionId, String clientId, String authenticationMethod, 
                          boolean authenticated, Map<String, Object> attributes) {
        this.sessionId = sessionId;
        this.clientId = clientId;
        this.authenticationMethod = authenticationMethod;
        this.authenticated = authenticated;
        this.attributes = attributes;
    }
    
    public String getSessionId() { return sessionId; }
    public String getClientId() { return clientId; }
    public String getAuthenticationMethod() { return authenticationMethod; }
    public boolean isAuthenticated() { return authenticated; }
    public Map<String, Object> getAttributes() { return attributes; }
}