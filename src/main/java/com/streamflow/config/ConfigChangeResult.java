package com.streamflow.config;

import com.streamflow.broker.BrokerNode;
import java.util.List;

public class ConfigChangeResult {
    private final boolean successful;
    private final ConfigChangeStatus status;
    private final List<BrokerNode> appliedBrokers;
    private final String errorMessage;
    
    public ConfigChangeResult(boolean successful, ConfigChangeStatus status, 
                             List<BrokerNode> appliedBrokers, String errorMessage) {
        this.successful = successful;
        this.status = status;
        this.appliedBrokers = appliedBrokers;
        this.errorMessage = errorMessage;
    }
    
    public boolean isSuccessful() { return successful; }
    public ConfigChangeStatus getStatus() { return status; }
    public List<BrokerNode> getAppliedBrokers() { return appliedBrokers; }
    public String getErrorMessage() { return errorMessage; }
}

enum ConfigChangeStatus {
    PENDING, VALIDATING, APPLYING, APPLIED, FAILED, ROLLED_BACK
}