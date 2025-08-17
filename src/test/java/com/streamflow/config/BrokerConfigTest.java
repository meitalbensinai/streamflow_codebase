package com.streamflow.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

class BrokerConfigTest {
    
    private Properties props;

    @BeforeEach
    void setUp() {
        props = new Properties();
    }

    @Test
    void testDefaultConfiguration() {
        BrokerConfig config = new BrokerConfig(new Properties());
        
        assertEquals(9092, config.getPort());
        assertEquals("localhost", config.getHostName());
        assertEquals("/tmp/streamflow-logs", config.getLogDir());
        assertEquals(3, config.getNumNetworkThreads());
        assertEquals(8, config.getNumIoThreads());
        assertTrue(config.isAutoCreateTopicsEnabled());
        assertTrue(config.isAutoLeaderRebalanceEnabled());
        assertTrue(config.isControlledShutdownEnabled());
        assertEquals(10000L, config.getReplicaLagTimeMaxMs());
    }

    @Test
    void testCustomConfiguration() {
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.HOST_NAME, "kafka.example.com");
        props.setProperty(BrokerConfig.PORT, "9093");
        props.setProperty(BrokerConfig.LOG_DIR, "/var/kafka-logs");
        props.setProperty(BrokerConfig.NUM_NETWORK_THREADS, "5");
        props.setProperty(BrokerConfig.NUM_IO_THREADS, "12");
        props.setProperty(BrokerConfig.AUTO_CREATE_TOPICS_ENABLE, "false");
        props.setProperty(BrokerConfig.REPLICA_LAG_TIME_MAX_MS, "15000");
        
        BrokerConfig config = new BrokerConfig(props);
        
        assertEquals(1, config.getBrokerId());
        assertEquals("kafka.example.com", config.getHostName());
        assertEquals(9093, config.getPort());
        assertEquals("/var/kafka-logs", config.getLogDir());
        assertEquals(5, config.getNumNetworkThreads());
        assertEquals(12, config.getNumIoThreads());
        assertFalse(config.isAutoCreateTopicsEnabled());
        assertEquals(15000L, config.getReplicaLagTimeMaxMs());
    }

    @Test
    void testBrokerIdConfiguration() {
        // Test valid broker ID
        props.setProperty(BrokerConfig.BROKER_ID, "5");
        BrokerConfig config = new BrokerConfig(props);
        assertEquals(5, config.getBrokerId());
        
        // Test default when not set
        BrokerConfig defaultConfig = new BrokerConfig(new Properties());
        assertEquals(-1, defaultConfig.getBrokerId()); // Default is -1 indicating not set
    }

    @Test
    void testPortConfiguration() {
        props.setProperty(BrokerConfig.PORT, "9093");
        BrokerConfig config = new BrokerConfig(props);
        assertEquals(9093, config.getPort());
        
        // Test invalid port
        props.setProperty(BrokerConfig.PORT, "invalid");
        BrokerConfig invalidConfig = new BrokerConfig(props);
        assertThrows(NumberFormatException.class, invalidConfig::getPort);
    }

    @Test
    void testBooleanConfiguration() {
        // Test true values
        props.setProperty(BrokerConfig.AUTO_CREATE_TOPICS_ENABLE, "true");
        props.setProperty(BrokerConfig.AUTO_LEADER_REBALANCE_ENABLE, "TRUE");
        props.setProperty(BrokerConfig.CONTROLLED_SHUTDOWN_ENABLE, "True");
        
        BrokerConfig config = new BrokerConfig(props);
        assertTrue(config.isAutoCreateTopicsEnabled());
        assertTrue(config.isAutoLeaderRebalanceEnabled());
        assertTrue(config.isControlledShutdownEnabled());
        
        // Test false values
        props.setProperty(BrokerConfig.AUTO_CREATE_TOPICS_ENABLE, "false");
        props.setProperty(BrokerConfig.AUTO_LEADER_REBALANCE_ENABLE, "FALSE");
        props.setProperty(BrokerConfig.CONTROLLED_SHUTDOWN_ENABLE, "False");
        
        config = new BrokerConfig(props);
        assertFalse(config.isAutoCreateTopicsEnabled());
        assertFalse(config.isAutoLeaderRebalanceEnabled());
        assertFalse(config.isControlledShutdownEnabled());
    }

    @Test
    void testLongConfiguration() {
        props.setProperty(BrokerConfig.REPLICA_LAG_TIME_MAX_MS, "20000");
        BrokerConfig config = new BrokerConfig(props);
        assertEquals(20000L, config.getReplicaLagTimeMaxMs());
        
        // Test invalid long
        props.setProperty(BrokerConfig.REPLICA_LAG_TIME_MAX_MS, "invalid");
        BrokerConfig invalidConfig = new BrokerConfig(props);
        assertThrows(NumberFormatException.class, invalidConfig::getReplicaLagTimeMaxMs);
    }

    @Test
    void testDerivedConfiguration() {
        props.setProperty(BrokerConfig.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS, "600");
        BrokerConfig config = new BrokerConfig(props);
        
        // Leader election interval should be derived from leader imbalance check interval
        assertEquals(600000L, config.getLeaderElectionIntervalMs()); // 600 seconds * 1000
    }

    @Test
    void testSetAndGetMethods() {
        BrokerConfig config = new BrokerConfig(new Properties());
        
        config.set("custom.property", "custom.value");
        assertEquals("custom.value", config.get("custom.property"));
        
        config.set("numeric.property", 42);
        assertEquals(42, config.get("numeric.property"));
    }

    @Test
    void testGetAllConfigs() {
        props.setProperty(BrokerConfig.BROKER_ID, "1");
        props.setProperty(BrokerConfig.HOST_NAME, "test.host");
        
        BrokerConfig config = new BrokerConfig(props);
        
        var allConfigs = config.getAllConfigs();
        assertTrue(allConfigs.containsKey(BrokerConfig.BROKER_ID));
        assertTrue(allConfigs.containsKey(BrokerConfig.HOST_NAME));
        assertTrue(allConfigs.containsKey(BrokerConfig.PORT)); // Should include defaults
        
        assertEquals("1", allConfigs.get(BrokerConfig.BROKER_ID));
        assertEquals("test.host", allConfigs.get(BrokerConfig.HOST_NAME));
    }

    @Test
    void testPartitionHealthCheckInterval() {
        BrokerConfig config = new BrokerConfig(new Properties());
        assertEquals(30000L, config.getPartitionHealthCheckIntervalMs());
    }
}