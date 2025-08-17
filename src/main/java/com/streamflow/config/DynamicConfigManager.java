package com.streamflow.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DynamicConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(DynamicConfigManager.class);
    
    private final BrokerConfig brokerConfig;
    private final Map<String, Object> pendingChanges;
    private final CopyOnWriteArrayList<ConfigChangeListener> listeners;
    
    public DynamicConfigManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.pendingChanges = new ConcurrentHashMap<>();
        this.listeners = new CopyOnWriteArrayList<>();
    }
    
    public void initialize() {
        logger.info("Initializing dynamic configuration manager");
    }
    
    public void shutdown() {
        logger.info("Shutting down dynamic configuration manager");
    }
    
    public boolean validateConfigChange(String key, Object value) {
        // Basic validation - needs full implementation
        logger.debug("Validating config change: {} = {}", key, value);
        return true;
    }
    
    public void proposeConfigChange(String key, Object value) {
        if (validateConfigChange(key, value)) {
            pendingChanges.put(key, value);
            logger.debug("Proposed config change: {} = {}", key, value);
        }
    }
    
    public void applyPendingChanges() {
        // Basic application - needs full implementation
        for (Map.Entry<String, Object> entry : pendingChanges.entrySet()) {
            brokerConfig.set(entry.getKey(), entry.getValue());
            notifyListeners(entry.getKey(), entry.getValue());
        }
        pendingChanges.clear();
        logger.info("Applied {} pending configuration changes", pendingChanges.size());
    }
    
    public void addConfigChangeListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }
    
    private void notifyListeners(String key, Object value) {
        for (ConfigChangeListener listener : listeners) {
            try {
                listener.onConfigChange(key, value);
            } catch (Exception e) {
                logger.error("Error notifying config change listener", e);
            }
        }
    }
    
    public interface ConfigChangeListener {
        void onConfigChange(String key, Object value);
    }
}