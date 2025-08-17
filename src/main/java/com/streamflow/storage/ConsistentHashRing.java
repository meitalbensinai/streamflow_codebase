package com.streamflow.storage;

import com.streamflow.broker.BrokerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class ConsistentHashRing {
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashRing.class);
    
    private final SortedMap<Long, BrokerNode> ring;
    private final Map<BrokerNode, Integer> virtualNodes;
    private final int virtualNodesPerBroker;
    
    public ConsistentHashRing(int virtualNodesPerBroker) {
        this.ring = new TreeMap<>();
        this.virtualNodes = new ConcurrentHashMap<>();
        this.virtualNodesPerBroker = virtualNodesPerBroker;
    }
    
    public void addNode(BrokerNode broker) {
        for (int i = 0; i < virtualNodesPerBroker; i++) {
            long hash = hash(broker.getBrokerId() + ":" + i);
            ring.put(hash, broker);
        }
        virtualNodes.put(broker, virtualNodesPerBroker);
        logger.debug("Added broker {} to consistent hash ring", broker.getBrokerId());
    }
    
    public void removeNode(BrokerNode broker) {
        for (int i = 0; i < virtualNodesPerBroker; i++) {
            long hash = hash(broker.getBrokerId() + ":" + i);
            ring.remove(hash);
        }
        virtualNodes.remove(broker);
        logger.debug("Removed broker {} from consistent hash ring", broker.getBrokerId());
    }
    
    public BrokerNode getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        
        long hash = hash(key);
        SortedMap<Long, BrokerNode> tailMap = ring.tailMap(hash);
        Long nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(nodeHash);
    }
    
    private long hash(String input) {
        // Simple hash function - could be enhanced with better distribution
        return input.hashCode() & 0x7FFFFFFF;
    }
    
    public int size() {
        return virtualNodes.size();
    }
}