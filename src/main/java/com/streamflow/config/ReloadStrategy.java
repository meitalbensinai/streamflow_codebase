package com.streamflow.config;

public enum ReloadStrategy {
    IMMEDIATE,
    GRADUAL_ROLLOUT,
    CANARY_DEPLOYMENT,
    SCHEDULED
}