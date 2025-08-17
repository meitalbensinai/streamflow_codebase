package com.streamflow.config;

import java.util.List;

public class ConfigValidationResult {
    private final boolean valid;
    private final List<String> errors;
    private final List<String> warnings;
    
    public ConfigValidationResult(boolean valid, List<String> errors, List<String> warnings) {
        this.valid = valid;
        this.errors = errors;
        this.warnings = warnings;
    }
    
    public boolean isValid() { return valid; }
    public List<String> getErrors() { return errors; }
    public List<String> getWarnings() { return warnings; }
}