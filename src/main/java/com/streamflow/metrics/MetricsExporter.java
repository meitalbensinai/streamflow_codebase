package com.streamflow.metrics;

import java.util.Map;

public interface MetricsExporter {
    Map<String, Object> export(Map<String, Object> metrics);
    ExportFormat getFormat();
    String getName();
}

// Concrete implementations for testing
class PrometheusExporter implements MetricsExporter {
    public Map<String, Object> export(Map<String, Object> metrics) {
        return Map.of("formatted_output", "# Prometheus metrics\\nstreamflow_messages_total 100");
    }
    public ExportFormat getFormat() { return ExportFormat.PROMETHEUS; }
    public String getName() { return "prometheus"; }
}

class JsonExporter implements MetricsExporter {
    public Map<String, Object> export(Map<String, Object> metrics) {
        return Map.of("partitions", metrics, "timestamp", System.currentTimeMillis());
    }
    public ExportFormat getFormat() { return ExportFormat.JSON; }
    public String getName() { return "json"; }
}

class CustomExporter implements MetricsExporter {
    private final String formatName;
    public CustomExporter(String formatName) { this.formatName = formatName; }
    public Map<String, Object> export(Map<String, Object> metrics) {
        return Map.of("custom_format", formatName, "data", metrics);
    }
    public ExportFormat getFormat() { return ExportFormat.CUSTOM; }
    public String getName() { return formatName; }
}

class FailingExporter implements MetricsExporter {
    public Map<String, Object> export(Map<String, Object> metrics) {
        throw new RuntimeException("Export failed");
    }
    public ExportFormat getFormat() { return ExportFormat.PROMETHEUS; }
    public String getName() { return "failing"; }
}