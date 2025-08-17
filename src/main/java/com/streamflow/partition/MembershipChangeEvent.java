package com.streamflow.partition;

import com.streamflow.broker.BrokerNode;

public class MembershipChangeEvent {
    public enum Type {
        BROKER_ADDED,
        BROKER_REMOVED,
        BROKER_FAILED
    }

    private final Type type;
    private final BrokerNode broker;
    private final long timestamp;

    public MembershipChangeEvent(Type type, BrokerNode broker) {
        this.type = type;
        this.broker = broker;
        this.timestamp = System.currentTimeMillis();
    }

    public Type getType() {
        return type;
    }

    public BrokerNode getBroker() {
        return broker;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("MembershipChangeEvent{type=%s, broker=%s, timestamp=%d}", 
                           type, broker, timestamp);
    }
}