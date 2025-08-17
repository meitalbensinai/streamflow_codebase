package com.streamflow.config;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerConfig {
    private final Map<String, Object> configs;

    // Default configuration keys
    public static final String BROKER_ID = "broker.id";
    public static final String HOST_NAME = "host.name";
    public static final String PORT = "port";
    public static final String LOG_DIR = "log.dir";
    public static final String NUM_NETWORK_THREADS = "num.network.threads";
    public static final String NUM_IO_THREADS = "num.io.threads";
    public static final String SOCKET_SEND_BUFFER_BYTES = "socket.send.buffer.bytes";
    public static final String SOCKET_RECEIVE_BUFFER_BYTES = "socket.receive.buffer.bytes";
    public static final String SOCKET_REQUEST_MAX_BYTES = "socket.request.max.bytes";
    public static final String NUM_PARTITIONS = "num.partitions";
    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR = "num.recovery.threads.per.data.dir";
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";
    public static final String TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "transaction.state.log.replication.factor";
    public static final String TRANSACTION_STATE_LOG_MIN_ISR = "transaction.state.log.min.isr";
    public static final String LOG_FLUSH_INTERVAL_MESSAGES = "log.flush.interval.messages";
    public static final String LOG_FLUSH_INTERVAL_MS = "log.flush.interval.ms";
    public static final String LOG_RETENTION_HOURS = "log.retention.hours";
    public static final String LOG_RETENTION_BYTES = "log.retention.bytes";
    public static final String LOG_SEGMENT_BYTES = "log.segment.bytes";
    public static final String LOG_RETENTION_CHECK_INTERVAL_MS = "log.retention.check.interval.ms";
    public static final String REPLICA_FETCH_MAX_BYTES = "replica.fetch.max.bytes";
    public static final String REPLICA_FETCH_WAIT_MAX_MS = "replica.fetch.wait.max.ms";
    public static final String REPLICA_FETCH_MIN_BYTES = "replica.fetch.min.bytes";
    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS = "replica.high.watermark.checkpoint.interval.ms";
    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS = "fetch.purgatory.purge.interval.requests";
    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS = "producer.purgatory.purge.interval.requests";
    public static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";
    public static final String AUTO_LEADER_REBALANCE_ENABLE = "auto.leader.rebalance.enable";
    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS = "leader.imbalance.check.interval.seconds";
    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE = "leader.imbalance.per.broker.percentage";
    public static final String REPLICA_LAG_TIME_MAX_MS = "replica.lag.time.max.ms";
    public static final String REPLICA_SOCKET_TIMEOUT_MS = "replica.socket.timeout.ms";
    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES = "replica.socket.receive.buffer.bytes";
    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES = "replica.fetch.response.max.bytes";
    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES = "controlled.shutdown.max.retries";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS = "controlled.shutdown.retry.backoff.ms";
    public static final String CONTROLLED_SHUTDOWN_ENABLE = "controlled.shutdown.enable";

    public BrokerConfig(Properties props) {
        this.configs = new ConcurrentHashMap<>();
        
        // Set defaults
        setDefaults();
        
        // Override with provided properties
        for (String key : props.stringPropertyNames()) {
            configs.put(key, props.getProperty(key));
        }
    }

    private void setDefaults() {
        configs.put(PORT, 9092);
        configs.put(HOST_NAME, "localhost");
        configs.put(LOG_DIR, "/tmp/streamflow-logs");
        configs.put(NUM_NETWORK_THREADS, 3);
        configs.put(NUM_IO_THREADS, 8);
        configs.put(SOCKET_SEND_BUFFER_BYTES, 102400);
        configs.put(SOCKET_RECEIVE_BUFFER_BYTES, 102400);
        configs.put(SOCKET_REQUEST_MAX_BYTES, 104857600);
        configs.put(NUM_PARTITIONS, 1);
        configs.put(NUM_RECOVERY_THREADS_PER_DATA_DIR, 1);
        configs.put(OFFSETS_TOPIC_REPLICATION_FACTOR, 3);
        configs.put(TRANSACTION_STATE_LOG_REPLICATION_FACTOR, 3);
        configs.put(TRANSACTION_STATE_LOG_MIN_ISR, 2);
        configs.put(LOG_FLUSH_INTERVAL_MESSAGES, 10000);
        configs.put(LOG_FLUSH_INTERVAL_MS, 1000);
        configs.put(LOG_RETENTION_HOURS, 168);
        configs.put(LOG_RETENTION_BYTES, 1073741824);
        configs.put(LOG_SEGMENT_BYTES, 1073741824);
        configs.put(LOG_RETENTION_CHECK_INTERVAL_MS, 300000);
        configs.put(REPLICA_FETCH_MAX_BYTES, 1048576);
        configs.put(REPLICA_FETCH_WAIT_MAX_MS, 500);
        configs.put(REPLICA_FETCH_MIN_BYTES, 1);
        configs.put(REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS, 5000);
        configs.put(FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS, 1000);
        configs.put(PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS, 1000);
        configs.put(AUTO_CREATE_TOPICS_ENABLE, true);
        configs.put(AUTO_LEADER_REBALANCE_ENABLE, true);
        configs.put(LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS, 300);
        configs.put(LEADER_IMBALANCE_PER_BROKER_PERCENTAGE, 10);
        configs.put(REPLICA_LAG_TIME_MAX_MS, 10000);
        configs.put(REPLICA_SOCKET_TIMEOUT_MS, 30000);
        configs.put(REPLICA_SOCKET_RECEIVE_BUFFER_BYTES, 65536);
        configs.put(REPLICA_FETCH_RESPONSE_MAX_BYTES, 10485760);
        configs.put(CONTROLLED_SHUTDOWN_MAX_RETRIES, 3);
        configs.put(CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS, 5000);
        configs.put(CONTROLLED_SHUTDOWN_ENABLE, true);
    }

    public int getBrokerId() {
        return getInt(BROKER_ID, -1);
    }

    public String getHostName() {
        return getString(HOST_NAME);
    }

    public int getPort() {
        return getInt(PORT);
    }

    public String getLogDir() {
        return getString(LOG_DIR);
    }

    public int getNumNetworkThreads() {
        return getInt(NUM_NETWORK_THREADS);
    }

    public int getNumIoThreads() {
        return getInt(NUM_IO_THREADS);
    }

    public long getReplicaLagTimeMaxMs() {
        return getLong(REPLICA_LAG_TIME_MAX_MS);
    }

    public long getLeaderElectionIntervalMs() {
        return getLong(LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS) * 1000;
    }

    public long getPartitionHealthCheckIntervalMs() {
        return 30000L; // 30 seconds
    }

    public boolean isAutoCreateTopicsEnabled() {
        return getBoolean(AUTO_CREATE_TOPICS_ENABLE);
    }

    public boolean isAutoLeaderRebalanceEnabled() {
        return getBoolean(AUTO_LEADER_REBALANCE_ENABLE);
    }

    public boolean isControlledShutdownEnabled() {
        return getBoolean(CONTROLLED_SHUTDOWN_ENABLE);
    }

    // Utility methods
    private String getString(String key) {
        return (String) configs.get(key);
    }

    private int getInt(String key) {
        Object value = configs.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Invalid integer value for key: " + key);
    }

    private int getInt(String key, int defaultValue) {
        try {
            return getInt(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getLong(String key) {
        Object value = configs.get(key);
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Invalid long value for key: " + key);
    }

    private boolean getBoolean(String key) {
        Object value = configs.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException("Invalid boolean value for key: " + key);
    }

    public void set(String key, Object value) {
        configs.put(key, value);
    }

    // Public getter methods for patches
    public int getInt(String key) {
        Object value = configs.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Invalid integer value for key: " + key);
    }

    public long getLong(String key) {
        Object value = configs.get(key);
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Invalid long value for key: " + key);
    }

    public boolean getBoolean(String key) {
        Object value = configs.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException("Invalid boolean value for key: " + key);
    }

    public Object get(String key) {
        return configs.get(key);
    }

    public Map<String, Object> getAllConfigs() {
        return Map.copyOf(configs);
    }
}