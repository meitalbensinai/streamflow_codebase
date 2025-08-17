package com.streamflow.config;

import java.time.Duration;

public class RollbackOptions {
    private boolean validateCompatibility = true;
    private boolean gradualRollback = false;
    private Duration maxRollbackTime = Duration.ofMinutes(10);
    
    public RollbackOptions setValidateCompatibility(boolean validateCompatibility) {
        this.validateCompatibility = validateCompatibility;
        return this;
    }
    
    public RollbackOptions setGradualRollback(boolean gradualRollback) {
        this.gradualRollback = gradualRollback;
        return this;
    }
    
    public RollbackOptions setMaxRollbackTime(Duration maxRollbackTime) {
        this.maxRollbackTime = maxRollbackTime;
        return this;
    }
    
    public boolean isValidateCompatibility() { return validateCompatibility; }
    public boolean isGradualRollback() { return gradualRollback; }
    public Duration getMaxRollbackTime() { return maxRollbackTime; }
}