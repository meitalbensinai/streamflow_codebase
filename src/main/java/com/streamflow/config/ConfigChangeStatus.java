package com.streamflow.config;

public enum ConfigChangeStatus {
    PENDING, VALIDATING, APPLYING, APPLIED, FAILED, ROLLED_BACK
}