package com.streamflow.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Central registry for dynamic configuration providers
 * Different components register their configuration suppliers here
 */
public class ConfigurationRegistry {
    private static final ConfigurationRegistry INSTANCE = new ConfigurationRegistry();
    
    private final Map<String, Supplier<Object>> configProviders = new ConcurrentHashMap<>();
    private final Map<String, String> configMappings = new ConcurrentHashMap<>();
    
    private ConfigurationRegistry() {
        initializeDefaultMappings();
    }
    
    public static ConfigurationRegistry getInstance() {
        return INSTANCE;
    }
    
    private void initializeDefaultMappings() {
        // Hidden mapping that connects broker config to other systems
        configMappings.put("broker.replication.sync.mode", "replication.coordinator.sync.strategy");
        configMappings.put("broker.partition.health.strategy", "partition.monitor.health.check.mode");
        configMappings.put("broker.storage.compression.threshold", "storage.engine.compression.policy.threshold");
        configMappings.put("broker.leader.election.timeout", "consensus.leader.election.timeout.ms");
        configMappings.put("broker.message.routing.strategy", "network.message.dispatcher.routing.algorithm");
    }
    
    public void registerConfigProvider(String key, Supplier<Object> provider) {
        configProviders.put(key, provider);
    }
    
    public Object getConfigValue(String key) {
        // First check direct providers
        Supplier<Object> provider = configProviders.get(key);
        if (provider != null) {
            return provider.get();
        }
        
        // Then check mapped keys
        String mappedKey = configMappings.get(key);
        if (mappedKey != null) {
            provider = configProviders.get(mappedKey);
            if (provider != null) {
                return provider.get();
            }
        }
        
        return null;
    }
    
    public boolean hasConfigProvider(String key) {
        return configProviders.containsKey(key) || 
               (configMappings.containsKey(key) && 
                configProviders.containsKey(configMappings.get(key)));
    }
    
    public void addConfigMapping(String sourceKey, String targetKey) {
        configMappings.put(sourceKey, targetKey);
    }
}