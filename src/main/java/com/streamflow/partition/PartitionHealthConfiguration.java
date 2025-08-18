package com.streamflow.partition;

/**
 * ANOTHER DECOY CLASS
 * 
 * This class appears to contain partition health configuration but is completely unused.
 * Agents may waste time trying to modify this class thinking it controls health intervals.
 */
public class PartitionHealthConfiguration {
    
    // Misleading constants that suggest this class is important
    public static final String HEALTH_CHECK_MODE_ISR = "ISR_BASED";
    public static final String HEALTH_CHECK_MODE_LEADER = "LEADER_HEALTH";
    public static final String HEALTH_CHECK_MODE_ALL = "ALL_REPLICA_HEALTH";
    public static final String HEALTH_CHECK_MODE_BASIC = "BASIC";
    public static final String HEALTH_CHECK_MODE_OPTIMIZED = "OPTIMIZED";
    
    // Misleading interval mapping that agents might think they need to use
    private static final java.util.Map<String, Long> MODE_INTERVALS = java.util.Map.of(
        HEALTH_CHECK_MODE_ISR, 15000L,
        HEALTH_CHECK_MODE_LEADER, 30000L,
        HEALTH_CHECK_MODE_ALL, 10000L,
        HEALTH_CHECK_MODE_BASIC, 60000L,
        HEALTH_CHECK_MODE_OPTIMIZED, 5000L
    );
    
    public static long getIntervalForMode(String mode) {
        return MODE_INTERVALS.getOrDefault(mode, 30000L);
    }
    
    public static boolean isValidMode(String mode) {
        return MODE_INTERVALS.containsKey(mode);
    }
    
    // Method that looks like it should be called but isn't
    public static String determineOptimalMode(int replicationFactor, boolean autoRebalance) {
        if (replicationFactor >= 3 && autoRebalance) {
            return HEALTH_CHECK_MODE_OPTIMIZED;
        } else if (replicationFactor == 1) {
            return HEALTH_CHECK_MODE_LEADER;
        } else {
            return HEALTH_CHECK_MODE_ISR;
        }
    }
}