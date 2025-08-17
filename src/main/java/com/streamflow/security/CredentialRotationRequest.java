package com.streamflow.security;

import java.util.Map;

public class CredentialRotationRequest {
    private final String clientId;
    private final CredentialType credentialType;
    private final Map<String, String> parameters;
    
    public CredentialRotationRequest(String clientId, CredentialType credentialType, Map<String, String> parameters) {
        this.clientId = clientId;
        this.credentialType = credentialType;
        this.parameters = parameters;
    }
    
    public String getClientId() { return clientId; }
    public CredentialType getCredentialType() { return credentialType; }
    public Map<String, String> getParameters() { return parameters; }
}