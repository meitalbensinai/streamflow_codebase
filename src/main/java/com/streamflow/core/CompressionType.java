package com.streamflow.core;

public enum CompressionType {
    NONE("none"),
    GZIP("gzip"),
    LZ4("lz4"),
    SNAPPY("snappy");

    private final String name;

    CompressionType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static CompressionType fromString(String name) {
        for (CompressionType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        return NONE;
    }
}