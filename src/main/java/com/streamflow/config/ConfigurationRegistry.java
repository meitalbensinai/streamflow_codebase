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
        configMappings.put("replication.coordinator.sync.strategy", "broker.replication.sync.mode");
        configMappings.put("partition.monitor.health.check.mode", "broker.partition.health.strategy");
        configMappings.put("storage.engine.compression.policy.threshold", "broker.storage.compression.threshold");
        configMappings.put("consensus.leader.election.timeout.ms", "broker.leader.election.timeout");
        configMappings.put("network.message.dispatcher.routing.algorithm", "broker.message.routing.strategy");
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